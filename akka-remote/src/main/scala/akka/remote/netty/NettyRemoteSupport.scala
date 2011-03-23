/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.remote.netty

import akka.config.ConfigurationException
import akka.serialization.RemoteActorSerialization
import akka.serialization.RemoteActorSerialization._
import akka.japi.Creator
import akka.config.Config._
import akka.remoteinterface._
import akka.actor.{PoisonPill, Index,
                   ActorInitializationException, LocalActorRef, newUuid,
                   ActorRegistry, Actor, RemoteActorRef,
                   TypedActor, ActorRef, IllegalActorStateException,
                   RemoteActorSystemMessage, uuidFrom, Uuid,
                   Exit, LifeCycleMessage, ActorType => AkkaActorType}
import akka.AkkaException
import akka.actor.Actor._
import akka.util._
import akka.event.EventHandler
import akka.remote.{MessageSerializer, RemoteClientSettings, RemoteServerSettings}

import org.jboss.netty.channel._
import org.jboss.netty.channel.group.{DefaultChannelGroup,ChannelGroup,ChannelGroupFuture}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.bootstrap.{ServerBootstrap,ClientBootstrap}
import org.jboss.netty.handler.codec.frame.{ LengthFieldBasedFrameDecoder, LengthFieldPrepender }
import org.jboss.netty.handler.codec.compression.{ ZlibDecoder, ZlibEncoder }
import org.jboss.netty.handler.codec.protobuf.{ ProtobufDecoder, ProtobufEncoder }
import org.jboss.netty.handler.timeout.{ ReadTimeoutHandler, ReadTimeoutException }
import org.jboss.netty.util.{ TimerTask, Timeout, HashedWheelTimer }
import org.jboss.netty.handler.ssl.SslHandler

import scala.collection.mutable.{ HashMap }
import scala.reflect.BeanProperty

import java.net.{ SocketAddress, InetSocketAddress }
import java.lang.reflect.InvocationTargetException
import java.util.concurrent.{ TimeUnit, Executors, ConcurrentMap, ConcurrentHashMap, ConcurrentSkipListSet }
import java.util.concurrent.atomic.{AtomicReference, AtomicLong, AtomicBoolean}
import akka.remote._
import akka.dispatch.CompletableFuture
import protocol.RemoteProtocol.{AkkaRemoteProtocol, CommandType, RemoteControlProtocol, RemoteMessageProtocol}
import org.jboss.netty.handler.execution.{OrderedMemoryAwareThreadPoolExecutor, ExecutionHandler}

trait NettyRemoteClientModule extends DefaultRemoteClientModule {
  self: ListenerManagement =>
  protected[akka] def createClient(address: InetSocketAddress, loader: Option[ClassLoader]): RemoteClient = {
    new ActiveRemoteClient(this, address, loader, self.notifyListeners _)
  }
}

/**
 * This is the abstract baseclass for netty remote clients,
 * currently there's only an ActiveRemoteClient, but otehrs could be feasible, like a PassiveRemoteClient that
 * reuses an already established connection.
 */
abstract class RemoteClient private[akka](
                                             module: RemoteClientModule,
                                             remoteAddress: InetSocketAddress) extends DefaultClient {

  def writeOneWay[T](request: RemoteMessageProtocol): Unit = {
    val future = currentChannel.write(RemoteEncoder.encode(request))
        future.awaitUninterruptibly()
        if (!future.isCancelled && !future.isSuccess) {
          notifyListeners(RemoteClientWriteFailed(request, future.getCause, module, remoteAddress))
          throw future.getCause
        }
  }

  def writeTwoWay[T](request: RemoteMessageProtocol): Unit = {

    currentChannel.write(RemoteEncoder.encode(request)).addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        if (future.isCancelled) {
          removeFuture(request) //Clean this up
          //We don't care about that right now
        } else if (!future.isSuccess) {
          val f = removeFuture(request) //Clean this up
          if (f ne null)
            f.completeWithException(future.getCause)
          notifyListeners(RemoteClientWriteFailed(request, future.getCause, module, remoteAddress))
        }
      }
    })
  }
}

/**
 *  RemoteClient represents a connection to an Akka node. Is used to send messages to remote actors on the node.
 *
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class ActiveRemoteClient private[akka] (
  val module: NettyRemoteClientModule, val remoteAddress: InetSocketAddress,
  val loader: Option[ClassLoader] = None, notifyListenersFun: (=> Any) => Unit) extends RemoteClient(module, remoteAddress) {
  import RemoteClientSettings._
  //FIXME rewrite to a wrapper object (minimize volatile access and maximize encapsulation)
  @volatile private var bootstrap: ClientBootstrap = _
  @volatile private[remote] var connection: ChannelFuture = _
  @volatile private[remote] var openChannels: DefaultChannelGroup = _
  @volatile private var timer: HashedWheelTimer = _
  @volatile private var reconnectionTimeWindowStart = 0L

  def notifyListeners(msg: => Any): Unit = notifyListenersFun(msg)
  def currentChannel = connection.getChannel

  def connect(reconnectIfAlreadyConnected: Boolean = false): Boolean = {
    runSwitch switchOn {
      openChannels = new DefaultDisposableChannelGroup(classOf[RemoteClient].getName)
      timer = new HashedWheelTimer

      bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool))
      bootstrap.setPipelineFactory(new ActiveRemoteClientPipelineFactory(name, futures, supervisors, bootstrap, remoteAddress, timer, this))
      bootstrap.setOption("tcpNoDelay", true)
      bootstrap.setOption("keepAlive", true)

      // Wait until the connection attempt succeeds or fails.
      connection = bootstrap.connect(remoteAddress)
      openChannels.add(connection.awaitUninterruptibly.getChannel)

      if (!connection.isSuccess) {
        notifyListeners(RemoteClientError(connection.getCause, module, remoteAddress))
        false
      } else {
        //Add a task that does GCing of expired Futures
        timer.newTimeout(new TimerTask() {
          def run(timeout: Timeout) = {
            if(isRunning) {
              val i = futures.entrySet.iterator
              while(i.hasNext) {
                val e = i.next
                if (e.getValue.isExpired)
                  futures.remove(e.getKey)
              }
            }
          }
        }, RemoteClientSettings.REAP_FUTURES_DELAY.length, RemoteClientSettings.REAP_FUTURES_DELAY.unit)
        notifyListeners(RemoteClientStarted(module, remoteAddress))
        true
      }
    } match {
      case true => true
      case false if reconnectIfAlreadyConnected =>
        isAuthenticated.set(false)
        openChannels.remove(connection.getChannel)
        connection.getChannel.close
        connection = bootstrap.connect(remoteAddress)
        openChannels.add(connection.awaitUninterruptibly.getChannel) // Wait until the connection attempt succeeds or fails.
        if (!connection.isSuccess) {
          notifyListeners(RemoteClientError(connection.getCause, module, remoteAddress))
          false
        } else true
      case false => false
    }
  }

  //Please note that this method does _not_ remove the ARC from the NettyRemoteClientModule's map of clients
  def shutdown = runSwitch switchOff {
    notifyListeners(RemoteClientShutdown(module, remoteAddress))
    timer.stop
    timer = null
    openChannels.close.awaitUninterruptibly
    openChannels = null
    bootstrap.releaseExternalResources
    bootstrap = null
    connection = null
  }

  private[akka] def isWithinReconnectionTimeWindow: Boolean = {
    if (reconnectionTimeWindowStart == 0L) {
      reconnectionTimeWindowStart = System.currentTimeMillis
      true
    } else {
      /*Time left > 0*/ (RECONNECTION_TIME_WINDOW - (System.currentTimeMillis - reconnectionTimeWindowStart)) > 0
    }
  }

  private[akka] def resetReconnectionTimeWindow = reconnectionTimeWindowStart = 0L
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class ActiveRemoteClientPipelineFactory(
  name: String,
  futures: ConcurrentMap[Uuid, CompletableFuture[_]],
  supervisors: ConcurrentMap[Uuid, ActorRef],
  bootstrap: ClientBootstrap,
  remoteAddress: InetSocketAddress,
  timer: HashedWheelTimer,
  client: ActiveRemoteClient) extends ChannelPipelineFactory {

  def getPipeline: ChannelPipeline = {
    val timeout     = new ReadTimeoutHandler(timer, RemoteClientSettings.READ_TIMEOUT.length, RemoteClientSettings.READ_TIMEOUT.unit)
    val lenDec      = new LengthFieldBasedFrameDecoder(RemoteClientSettings.MESSAGE_FRAME_SIZE, 0, 4, 0, 4)
    val lenPrep     = new LengthFieldPrepender(4)
    val protobufDec = new ProtobufDecoder(AkkaRemoteProtocol.getDefaultInstance)
    val protobufEnc = new ProtobufEncoder
    val (enc, dec)  = RemoteServerSettings.COMPRESSION_SCHEME match {
      case   "zlib" => (new ZlibEncoder(RemoteServerSettings.ZLIB_COMPRESSION_LEVEL) :: Nil, new ZlibDecoder :: Nil)
      case        _ => (Nil,Nil)
    }

    val remoteClient = new ActiveRemoteClientHandler(name, futures, supervisors, bootstrap, remoteAddress, timer, client)
    val stages: List[ChannelHandler] = timeout :: dec ::: lenDec :: protobufDec :: enc ::: lenPrep :: protobufEnc :: remoteClient :: Nil
    new StaticChannelPipeline(stages: _*)
  }
}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@ChannelHandler.Sharable
class ActiveRemoteClientHandler(
                                   val name: String,
                                   val futures: ConcurrentMap[Uuid, CompletableFuture[_]],
                                   val supervisors: ConcurrentMap[Uuid, ActorRef],
                                   val bootstrap: ClientBootstrap,
                                   val remoteAddress: InetSocketAddress,
                                   val timer: HashedWheelTimer,
                                   val client: ActiveRemoteClient)
    extends SimpleChannelUpstreamHandler {

  override def messageReceived(ctx: ChannelHandlerContext, event: MessageEvent) {
    client.receiveMessage(event.getMessage)
  }

  override def channelClosed(ctx: ChannelHandlerContext, event: ChannelStateEvent) = client.runSwitch ifOn {
    if (client.isWithinReconnectionTimeWindow) {
      timer.newTimeout(new TimerTask() {
        def run(timeout: Timeout) = {
          if (client.isRunning) {
            client.openChannels.remove(event.getChannel)
            client.connect(reconnectIfAlreadyConnected = true)
          }
        }
      }, RemoteClientSettings.RECONNECT_DELAY.toMillis, TimeUnit.MILLISECONDS)
    } else spawn { client.module.shutdownClientConnection(remoteAddress) }
  }

  override def channelConnected(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    client.notifyListeners(RemoteClientConnected(client.module, client.remoteAddress))
    client.resetReconnectionTimeWindow
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    client.notifyListeners(RemoteClientDisconnected(client.module, client.remoteAddress))
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent) = {
    event.getCause match {
      case e: ReadTimeoutException =>
        spawn { client.module.shutdownClientConnection(remoteAddress) }
      case e =>
        client.notifyListeners(RemoteClientError(e, client.module, client.remoteAddress))
        event.getChannel.close //FIXME Is this the correct behavior?
    }
  }

}

/**
 * Provides the implementation of the Netty remote support
 */
class NettyRemoteSupport extends RemoteSupport with NettyRemoteServerModule with NettyRemoteClientModule {
  //Needed for remote testing and switching on/off under run
  val optimizeLocal = new AtomicBoolean(true)

  def optimizeLocalScoped_?() = optimizeLocal.get

  protected[akka] def actorFor(serviceId: String, className: String, timeout: Long, host: String, port: Int, loader: Option[ClassLoader]): ActorRef = {
    if (optimizeLocalScoped_?) {
      val home = this.address
      if ((host == home.getAddress.getHostAddress || host == home.getHostName) && port == home.getPort) {//TODO: switch to InetSocketAddress.equals?
        val localRef = findActorByIdOrUuid(serviceId,serviceId)
        if (localRef ne null) return localRef //Code significantly simpler with the return statement
      }
    }

    RemoteActorRef(serviceId, className, host, port, timeout, loader)
  }

  def clientManagedActorOf(factory: () => Actor, host: String, port: Int): ActorRef = {

    if (optimizeLocalScoped_?) {
      val home = this.address
      if ((host == home.getAddress.getHostAddress || host == home.getHostName) && port == home.getPort)//TODO: switch to InetSocketAddress.equals?
        return new LocalActorRef(factory, None) // Code is much simpler with return
    }

    val ref = new LocalActorRef(factory, Some(new InetSocketAddress(host, port)), clientManaged = true)
    //ref.timeout = timeout //removed because setting default timeout should be done after construction
    ref
  }
}

class NettyRemoteServer(serverModule: RemoteServerModule, val host: String, val port: Int, val loader: Option[ClassLoader]) extends RemoteServer {

  val name = "NettyRemoteServer@" + host + ":" + port
  val address = new InetSocketAddress(host, port)

  private val factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool, Executors.newCachedThreadPool)

  private val bootstrap = new ServerBootstrap(factory)

  // group of open channels, used for clean-up
  private val openChannels: ChannelGroup = new DefaultDisposableChannelGroup("akka-remote-server")
  val remoteServer = new RemoteServerHandler(name, openChannels, loader, serverModule)
  val pipelineFactory = new RemoteServerPipelineFactory(name, openChannels, loader, serverModule, remoteServer)
  bootstrap.setPipelineFactory(pipelineFactory)
  bootstrap.setOption("backlog", RemoteServerSettings.BACKLOG)
  bootstrap.setOption("child.tcpNoDelay", true)
  bootstrap.setOption("child.keepAlive", true)
  bootstrap.setOption("child.reuseAddress", true)
  bootstrap.setOption("child.connectTimeoutMillis", RemoteServerSettings.CONNECTION_TIMEOUT_MILLIS.toMillis)

  openChannels.add(bootstrap.bind(address))
  serverModule.notifyListeners(RemoteServerStarted(serverModule))

  def shutdown {
    try {
      val shutdownSignal = {
        val b = RemoteControlProtocol.newBuilder
        if (RemoteClientSettings.SECURE_COOKIE.nonEmpty)
          b.setCookie(RemoteClientSettings.SECURE_COOKIE.get)
        b.setCommandType(CommandType.SHUTDOWN)
        b.build
      }
      openChannels.write(RemoteEncoder.encode(shutdownSignal)).awaitUninterruptibly
      openChannels.disconnect
      openChannels.close.awaitUninterruptibly
      bootstrap.releaseExternalResources
      serverModule.notifyListeners(RemoteServerShutdown(serverModule))
    } catch {
      case e: Exception =>
        EventHandler.error(e, this, e.getMessage)
    }
  }

  protected [akka] def registerPipelines(pipelines:Pipelines):Unit = {
    remoteServer.registerPipelines(pipelines)
  }
  protected [akka] def unregisterPipelines():Unit = {
    remoteServer.unregisterPipelines()
  }
}

trait NettyRemoteServerModule extends DefaultRemoteServerModule {
  def createRemoteServer(remoteServerModule: RemoteServerModule, hostname: String, port: Int, loader: Option[ClassLoader]): RemoteServer = {
    new NettyRemoteServer(remoteServerModule, hostname, port, loader)
  }
}

object RemoteServerSslContext {

  import javax.net.ssl.SSLContext

  val (client, server) = {
    val protocol = "TLS"
    //val algorithm = Option(Security.getProperty("ssl.KeyManagerFactory.algorithm")).getOrElse("SunX509")
    //val store = KeyStore.getInstance("JKS")
    val s = SSLContext.getInstance(protocol)
    s.init(null, null, null)
    val c = SSLContext.getInstance(protocol)
    c.init(null, null, null)
    (c, s)
  }
}
/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
class RemoteServerPipelineFactory(
    val name: String,
    val openChannels: ChannelGroup,
    val loader: Option[ClassLoader],
    val server: RemoteServerModule,
    val remoteServerHandler: RemoteServerHandler) extends ChannelPipelineFactory {
  import RemoteServerSettings._

  def getPipeline: ChannelPipeline = {
    val lenDec      = new LengthFieldBasedFrameDecoder(MESSAGE_FRAME_SIZE, 0, 4, 0, 4)
    val lenPrep     = new LengthFieldPrepender(4)
    val protobufDec = new ProtobufDecoder(AkkaRemoteProtocol.getDefaultInstance)
    val protobufEnc = new ProtobufEncoder
    val (enc, dec)  = COMPRESSION_SCHEME match {
      case "zlib"  => (new ZlibEncoder(ZLIB_COMPRESSION_LEVEL) :: Nil, new ZlibDecoder :: Nil)
      case       _ => (Nil, Nil)
    }

    val execution = new ExecutionHandler(
      new OrderedMemoryAwareThreadPoolExecutor(
        EXECUTION_POOL_SIZE,
        MAX_CHANNEL_MEMORY_SIZE,
        MAX_TOTAL_MEMORY_SIZE,
        EXECUTION_POOL_KEEPALIVE.length,
        EXECUTION_POOL_KEEPALIVE.unit
      )
    )
    val stages: List[ChannelHandler] = dec ::: lenDec :: protobufDec :: enc ::: lenPrep :: protobufEnc :: execution :: remoteServerHandler :: Nil
    new StaticChannelPipeline(stages: _*)
  }
}

class NettySession(val server: RemoteServerModule, val channel: Channel) extends Session[Channel] {
  //Writes the specified message to the specified channel and propagates write errors to listeners
  def write(payload: AkkaRemoteProtocol): Unit = {
    channel.write(payload).addListener(
      new ChannelFutureListener {
        def operationComplete(future: ChannelFuture): Unit = {
          if (future.isCancelled) {
            //Not interesting at the moment
          } else if (!future.isSuccess) {
            val socketAddress = future.getChannel.getRemoteAddress match {
              case i: InetSocketAddress => Some(i)
              case _ => None
            }
            server.notifyListeners(RemoteServerWriteFailed(payload, future.getCause, server, socketAddress))
          }
        }
      })
  }

}

/**
 * @author <a href="http://jonasboner.com">Jonas Bon&#233;r</a>
 */
@ChannelHandler.Sharable
class RemoteServerHandler(
                             val name: String,
                             val openChannels: ChannelGroup,
                             val applicationLoader: Option[ClassLoader],
                             val server: RemoteServerModule) extends SimpleChannelUpstreamHandler with RemoteMessageProtocolHandler[Channel] {

  import RemoteServerSettings._

  val CHANNEL_INIT = "channel-init".intern

  val sessionActors = new ChannelLocal[ConcurrentHashMap[String, ActorRef]]()
  val typedSessionActors = new ChannelLocal[ConcurrentHashMap[String, AnyRef]]()

  def findSessionActor(id: String, channel: Channel) : ActorRef =
    sessionActors.get(channel) match {
      case null => null
      case map => map get id
    }

  def findTypedSessionActor(id: String, channel: Channel) : AnyRef =
    typedSessionActors.get(channel) match {
      case null => null
      case map => map get id
    }

  def getTypedSessionActors(channel: Channel): ConcurrentHashMap[String, AnyRef] = {
    typedSessionActors.get(channel)
  }

  def getSessionActors(channel: Channel): ConcurrentHashMap[String, ActorRef] = {
    sessionActors.get(channel)
  }

  /**
   * ChannelOpen overridden to store open channels for a clean postStop of a node.
   * If a channel is closed before, it is automatically removed from the open channels group.
   */
  override def channelOpen(ctx: ChannelHandlerContext, event: ChannelStateEvent) = openChannels.add(ctx.getChannel)

  override def channelConnected(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    val clientAddress = getClientAddress(ctx)
    sessionActors.set(event.getChannel(), new ConcurrentHashMap[String, ActorRef]())
    typedSessionActors.set(event.getChannel(), new ConcurrentHashMap[String, AnyRef]())
    server.notifyListeners(RemoteServerClientConnected(server, clientAddress))
    if (REQUIRE_COOKIE) ctx.setAttachment(CHANNEL_INIT) // signal that this is channel initialization, which will need authentication
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    import scala.collection.JavaConversions.asScalaIterable
    val clientAddress = getClientAddress(ctx)

    // stop all session actors
    for (map <- Option(sessionActors.remove(event.getChannel));
         actor <- asScalaIterable(map.values)) {
         try { actor ! PoisonPill } catch { case e: Exception => }
    }

    //FIXME switch approach or use other thread to execute this
    // stop all typed session actors
    for (map <- Option(typedSessionActors.remove(event.getChannel));
         actor <- asScalaIterable(map.values)) {
         try { TypedActor.stop(actor) } catch { case e: Exception => }
    }

    server.notifyListeners(RemoteServerClientDisconnected(server, clientAddress))
  }

  override def channelClosed(ctx: ChannelHandlerContext, event: ChannelStateEvent) = {
    val clientAddress = getClientAddress(ctx)
    server.notifyListeners(RemoteServerClientClosed(server, clientAddress))
  }

  override def messageReceived(ctx: ChannelHandlerContext, event: MessageEvent) = event.getMessage match {
    case null => throw new IllegalActorStateException("Message in remote MessageEvent is null: " + event)
    //case remoteProtocol: AkkaRemoteProtocol if remoteProtocol.hasInstruction => RemoteServer cannot receive control messages (yet)
    case remoteProtocol: AkkaRemoteProtocol if remoteProtocol.hasMessage =>
      val requestProtocol = remoteProtocol.getMessage
      if (REQUIRE_COOKIE) authenticateRemoteClient(requestProtocol, ctx)
      handleRemoteMessageProtocol(requestProtocol, new NettySession(server, event.getChannel))
    case _ => //ignore
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, event: ExceptionEvent) = {
    event.getChannel.close
    server.notifyListeners(RemoteServerError(event.getCause, server))
  }

  private def getClientAddress(ctx: ChannelHandlerContext): Option[InetSocketAddress] =
    ctx.getChannel.getRemoteAddress match {
      case inet: InetSocketAddress => Some(inet)
      case _ => None
    }

  private def authenticateRemoteClient(request: RemoteMessageProtocol, ctx: ChannelHandlerContext) = {
    val attachment = ctx.getAttachment
    if ((attachment ne null) &&
        attachment.isInstanceOf[String] &&
        attachment.asInstanceOf[String] == CHANNEL_INIT) { // is first time around, channel initialization
      ctx.setAttachment(null)
      val clientAddress = ctx.getChannel.getRemoteAddress.toString
      if (!request.hasCookie) throw new SecurityException(
        "The remote client [" + clientAddress + "] does not have a secure cookie.")
      if (!(request.getCookie == SECURE_COOKIE.get)) throw new SecurityException(
        "The remote client [" + clientAddress + "] secure cookie is not the same as remote server secure cookie")
    }
  }
}

class DefaultDisposableChannelGroup(name: String) extends DefaultChannelGroup(name) {
  protected val guard = new ReadWriteGuard
  protected val open  = new AtomicBoolean(true)

  override def add(channel: Channel): Boolean = guard withReadGuard {
    if(open.get) {
      super.add(channel)
    } else {
      channel.close
      false
    }
  }

  override def close(): ChannelGroupFuture = guard withWriteGuard {
    if (open.getAndSet(false)) {
      super.close
    } else {
      throw new IllegalStateException("ChannelGroup already closed, cannot add new channel")
    }
  }
}
