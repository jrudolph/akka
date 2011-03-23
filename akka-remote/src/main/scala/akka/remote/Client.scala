/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.remote

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import org.jboss.netty.channel.Channel
import akka.remoteinterface._
import protocol.RemoteProtocol.{CommandType, AkkaRemoteProtocol, RemoteMessageProtocol}
import scala.collection.mutable.HashMap
import akka.util._
import akka.event.EventHandler
import akka.actor.Actor._
import akka.actor.{TypedActor, Index, newUuid, Uuid, uuidFrom, Exit, IllegalActorStateException, ActorRef, ActorType => AkkaActorType, RemoteActorRef}
import akka.serialization.RemoteActorSerialization
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import akka.dispatch.{DefaultCompletableFuture, CompletableFuture}

/**
 * remote client interface that needs to be implemented for a specific transport
 */
trait Client {
  def connect(reconnectIfAlreadyConnected: Boolean = false): Boolean

  def shutdown: Boolean

  def send[T](
                 message: Any,
                 senderOption: Option[ActorRef],
                 senderFuture: Option[CompletableFuture[T]],
                 remoteAddress: InetSocketAddress,
                 timeout: Long,
                 isOneWay: Boolean,
                 actorRef: ActorRef,
                 typedActorInfo: Option[Tuple2[String, String]],
                 actorType: AkkaActorType): Option[CompletableFuture[T]]

  protected[akka] def registerPipelines(pipelines: Pipelines): Unit

  protected[akka] def unregisterPipelines(): Unit

  protected def notifyListeners(msg: => Any)

  protected[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef

  protected[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef
}

/**
 * ClientFilters, as used in remote.clientFilters. registers itself on the client module through a ClientFilterRegistry
 */
private[akka] class ClientFilters(val address: Address, clientFilterRegistry: ClientFilterRegistry) extends Filters with Pipelines {

  private[akka] def update() = {
    clientFilterRegistry.registerClientFilters(address, this)
  }

  private[akka] def unregister() = {
    clientFilterRegistry.unregisterClientFilters(address)
  }
}

/**
 * Default base for remote clients.
 */
trait DefaultClient extends Client {
  val remoteAddress: InetSocketAddress
  val module: DefaultRemoteClientModule
  protected val loader: Option[ClassLoader]
  protected val currentPipelines = new AtomicReference[Option[Pipelines]](None)
  val name = this.getClass.getSimpleName + "@" + remoteAddress.getHostName + "::" + remoteAddress.getPort

  protected val futures = new ConcurrentHashMap[Uuid, CompletableFuture[_]]
  protected val supervisors = new ConcurrentHashMap[Uuid, ActorRef]
  private[remote] val runSwitch = new Switch()
  private[remote] val isAuthenticated = new AtomicBoolean(false)

  private[remote] def isRunning = runSwitch.isOn

  protected def notifyListeners(msg: => Any): Unit

  protected def currentChannel: Channel

  protected[akka] def registerPipelines(pipelines: Pipelines): Unit = {
    this.currentPipelines.set(Some(pipelines))
  }

  protected[akka] def unregisterPipelines(): Unit = {
    this.currentPipelines.set(None)
  }

  def connect(reconnectIfAlreadyConnected: Boolean = false): Boolean

  def shutdown: Boolean

  /**
   * Converts the message to the wireprotocol and sends the message across the wire
   */
  def send[T](
                 message: Any,
                 senderOption: Option[ActorRef],
                 senderFuture: Option[CompletableFuture[T]],
                 remoteAddress: InetSocketAddress,
                 timeout: Long,
                 isOneWay: Boolean,
                 actorRef: ActorRef,
                 typedActorInfo: Option[Tuple2[String, String]],
                 actorType: AkkaActorType): Option[CompletableFuture[T]] = synchronized {
    //TODO: find better strategy to prevent race
    val builder = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
      Some(actorRef),
      Left(actorRef.uuid),
      actorRef.id,
      actorRef.actorClassName,
      actorRef.timeout,
      Right(message),
      isOneWay,
      senderOption,
      typedActorInfo,
      actorType,
      if (isAuthenticated.compareAndSet(false, true)) RemoteClientSettings.SECURE_COOKIE else None
    )

    whenRunning {
      currentPipelines.get match {
        case Some(pipelines) => {
          if (pipelines.isOutgoingEmpty) {
            send(builder, senderFuture)
          } else {
            pipelines.outgoing(builder) match {
              case Some(resultBuilder: RemoteMessageProtocol.Builder) => {
                send(resultBuilder, senderFuture)
              }
              case None => {
                if (!builder.getOneWay) {
                  // check if the client is expecting a reply, and return a future so the client can timeout.
                  val futureUuid = uuidFrom(builder.getUuid.getHigh, builder.getUuid.getLow)
                  val futureResult = getOrCreateFuture(senderFuture, futureUuid, builder.getActorInfo.getTimeout)
                  Some(futureResult)
                } else {
                  None
                }
              }
            }
          }
        }
        case None => {
          send(builder, senderFuture)
        }
      }
    }
  }

  private def send[T](builder: RemoteMessageProtocol.Builder, senderFuture: Option[CompletableFuture[T]]): Option[CompletableFuture[T]] = {
    val request = builder.build
    if (request.getOneWay) {
      writeOneWay(request)
      None
    } else {
      val futureUuid = uuidFrom(request.getUuid.getHigh, request.getUuid.getLow)
      val futureResult = getOrCreateFuture(senderFuture, futureUuid, request.getActorInfo.getTimeout)
      futures.put(futureUuid, futureResult) //Add this prematurely, writeTwoWay has to remove it if write fails
      writeTwoWay(request)
      Some(futureResult)
    }
  }

  /**
   * Sends the message across the wire
   */
  def writeOneWay[T](request: RemoteMessageProtocol): Unit

  def writeTwoWay[T](request: RemoteMessageProtocol): Unit

  /**
   * removeFuture needs to be called from writeTwoWay if the sending was cancelled or no success
   */
  protected def removeFuture(request: RemoteMessageProtocol): CompletableFuture[_] = {
    val futureUuid = uuidFrom(request.getUuid.getHigh, request.getUuid.getLow)
    futures.remove(futureUuid)
  }

  private def getOrCreateFuture[T](senderFuture: Option[CompletableFuture[T]], futureUuid: Uuid, timeout: Long): CompletableFuture[T] = {
    val futureResult = if (senderFuture.isDefined) senderFuture.get else new DefaultCompletableFuture[T](timeout)
    futureResult
  }

  private def whenRunning[T](body: => T): T = {
    if (isRunning) {
      body
    } else {
      val exception = new RemoteClientException("Remote client is not running, make sure you have invoked 'RemoteClient.connect' before using it.", module, remoteAddress)
      notifyListeners(RemoteClientError(exception, module, remoteAddress))
      throw exception
    }
  }

  private def receive(reply: RemoteMessageProtocol, future: CompletableFuture[Any], replyUuid: Uuid): CompletableFuture[Any] = {
    if (reply.hasMessage) {
      if (future eq null) throw new IllegalActorStateException("Future mapped to UUID " + replyUuid + " does not exist")
      val message = MessageSerializer.deserialize(reply.getMessage)
      future.completeWithResult(message)
    } else {
      val exception = parseException(reply, loader)

      if (reply.hasSupervisorUuid()) {
        val supervisorUuid = uuidFrom(reply.getSupervisorUuid.getHigh, reply.getSupervisorUuid.getLow)
        if (!supervisors.containsKey(supervisorUuid)) throw new IllegalActorStateException(
          "Expected a registered supervisor for UUID [" + supervisorUuid + "] but none was found")
        val supervisedActor = supervisors.get(supervisorUuid)
        if (!supervisedActor.supervisor.isDefined) throw new IllegalActorStateException(
          "Can't handle restart for remote actor " + supervisedActor + " since its supervisor has been removed")
        else supervisedActor.supervisor.get ! Exit(supervisedActor, exception)
      }

      future.completeWithException(exception)
    }
  }

  def receiveMessage(message: AnyRef): Unit = {
    try {
      message match {
        case arp: AkkaRemoteProtocol if arp.hasInstruction =>
          val rcp = arp.getInstruction
          rcp.getCommandType match {
            case CommandType.SHUTDOWN => spawn { module.shutdownClientConnection(remoteAddress) }
          }
        case arp: AkkaRemoteProtocol if arp.hasMessage =>
          val reply = arp.getMessage
          val replyUuid = uuidFrom(reply.getActorInfo.getUuid.getHigh, reply.getActorInfo.getUuid.getLow)
          val future = futures.remove(replyUuid).asInstanceOf[CompletableFuture[Any]]
          currentPipelines.get match {
            case Some(pipelines) => {
              if (pipelines.isIncomingEmpty) {
                receive(reply, future, replyUuid)
              } else {
                pipelines.incoming(reply.toBuilder) foreach {
                  builder =>
                    receive(builder.build, future, replyUuid)
                }
              }
            }
            case None => receive(reply, future, replyUuid)
          }
        case other =>
          throw new RemoteClientException("Unknown message received in remote client handler: " + other, module, remoteAddress)
      }
    } catch {
      case e: Throwable =>
        EventHandler.error(e, this, e.getMessage)
        notifyListeners(RemoteClientError(e, module, remoteAddress))
        throw e
    }
  }

  private def parseException(reply: RemoteMessageProtocol, loader: Option[ClassLoader]): Throwable = {
    val exception = reply.getException
    val classname = exception.getClassname
    try {
      val exceptionClass = if (loader.isDefined) loader.get.loadClass(classname)
      else Class.forName(classname)
      exceptionClass
          .getConstructor(Array[Class[_]](classOf[String]): _*)
          .newInstance(exception.getMessage).asInstanceOf[Throwable]
    } catch {
      case problem: Throwable =>
        EventHandler.error(problem, this, problem.getMessage)
        UnparsableException(classname, exception.getMessage)
    }
  }

  protected[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef =
    if (!actorRef.supervisor.isDefined) throw new IllegalActorStateException(
      "Can't register supervisor for " + actorRef + " since it is not under supervision")
    else supervisors.putIfAbsent(actorRef.supervisor.get.uuid, actorRef)

  protected[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef =
    if (!actorRef.supervisor.isDefined) throw new IllegalActorStateException(
      "Can't unregister supervisor for " + actorRef + " since it is not under supervision")
    else supervisors.remove(actorRef.supervisor.get.uuid)
}

/**
 *  an interface to the client module to register and unregister filters
 */
private [akka] trait ClientFilterRegistry {
  private[akka] def registerClientFilters(remoteAddress: Address, filters: ClientFilters): Unit

  private[akka] def unregisterClientFilters(remoteAddress: Address): Unit
}

/**
 * Default base for remote client modules
 */
trait DefaultRemoteClientModule extends RemoteClientModule with ClientFilterRegistry {
  self: ListenerManagement =>
  private val remoteClients = new HashMap[Address, Client]
  private val remoteActors = new Index[Address, Uuid]
  private val filtersMap = new HashMap[Address, ClientFilters]
  private val lock = new ReadWriteGuard
  private val filtersLock = new ReadWriteGuard

  protected[akka] def typedActorFor[T](intfClass: Class[T], serviceId: String, implClassName: String, timeout: Long, hostname: String, port: Int, loader: Option[ClassLoader]): T =
    TypedActor.createProxyForRemoteActorRef(intfClass, RemoteActorRef(serviceId, implClassName, hostname, port, timeout, loader, AkkaActorType.TypedActor))

  protected[akka] def send[T](message: Any,
                              senderOption: Option[ActorRef],
                              senderFuture: Option[CompletableFuture[T]],
                              remoteAddress: InetSocketAddress,
                              timeout: Long,
                              isOneWay: Boolean,
                              actorRef: ActorRef,
                              typedActorInfo: Option[Tuple2[String, String]],
                              actorType: AkkaActorType,
                              loader: Option[ClassLoader]): Option[CompletableFuture[T]] =
    withClientFor(remoteAddress, loader)(_.send[T](message, senderOption, senderFuture, remoteAddress, timeout, isOneWay, actorRef, typedActorInfo, actorType))

  private[akka] def withClientFor[T](
                                        address: InetSocketAddress, loader: Option[ClassLoader])(fun: Client => T): T = {
    loader.foreach(MessageSerializer.setClassLoader(_)) //TODO: REVISIT: THIS SMELLS FUNNY
    //FIXME if address is entered as (hostname, port) it will not be found by hostaddress and port
    val key = Address(address)
    lock.readLock.lock
    try {
      val c = remoteClients.get(key) match {
        case Some(client) => {
          updateFilters(client, key)
          client
        }
        case None =>
          lock.readLock.unlock
          lock.writeLock.lock //Lock upgrade, not supported natively
          try {
            try {
              remoteClients.get(key) match {
              //Recheck for addition, race between upgrades
                case Some(client) => client //If already populated by other writer
                case None => {
                  //Populate map
                  val client = createClient(address, loader)
                  client.connect()
                  remoteClients += key -> client
                  updateFilters(client, key)
                  client
                }
              }
            } finally {
              lock.readLock.lock
            } //downgrade
          } finally {
            lock.writeLock.unlock
          }
      }
      fun(c)
    } finally {
      lock.readLock.unlock
    }
  }

  def shutdownClientConnection(address: InetSocketAddress): Boolean = lock withWriteGuard {
    remoteClients.remove(Address(address)) match {
      case s: Some[Client] => s.get.shutdown
      case None => false
    }
  }

  def restartClientConnection(address: InetSocketAddress): Boolean = lock withReadGuard {
    remoteClients.get(Address(address)) match {
      case s: Some[Client] => s.get.connect(reconnectIfAlreadyConnected = true)
      case None => false
    }
  }

  private[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef =
    withClientFor(actorRef.homeAddress.get, None)(_.registerSupervisorForActor(actorRef))

  private[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef = lock withReadGuard {
    remoteClients.get(Address(actorRef.homeAddress.get)) match {
      case s: Some[Client] => s.get.deregisterSupervisorForActor(actorRef)
      case None => actorRef
    }
  }

  /**
   * Clean-up all open connections.
   */
  def shutdownClientModule = {
    shutdownRemoteClients
    //TODO: Should we empty our remoteActors too?
    //remoteActors.clear
  }

  def shutdownRemoteClients = lock withWriteGuard {
    remoteClients.foreach({
      case (addr, client) => client.shutdown
    })
    remoteClients.clear
  }

  def registerClientManagedActor(hostname: String, port: Int, uuid: Uuid) = {
    remoteActors.put(Address(hostname, port), uuid)
  }

  private[akka] def unregisterClientManagedActor(hostname: String, port: Int, uuid: Uuid) = {
    remoteActors.remove(Address(hostname, port), uuid)
    //TODO: should the connection be closed when the last actor deregisters?
  }

  def clientFilters(hostname:String, port:Int): Filters = {
    val remoteAddress = Address(new InetSocketAddress(hostname, port))

    filtersLock.readLock.lock
    try {
      filtersMap.get(remoteAddress) match {
        case Some(filters) => {
          filters
        }
        case None => {
          filtersLock.readLock.unlock
          try {
            filtersLock.writeLock.lock
            try {
              val newFilters = new ClientFilters(remoteAddress, this)
              filtersMap.update(remoteAddress, newFilters)
              newFilters
            } finally {
              filtersLock.writeLock.unlock
            }
          } finally {
            // so that finally at the end can always unlock
            filtersLock.readLock.lock
          }
        }
      }
    } finally {
      filtersLock.readLock.unlock
    }
  }

  private[akka] def registerClientFilters(remoteAddress: Address, filters: ClientFilters): Unit = filtersLock.withWriteGuard {
    filtersMap.update(remoteAddress, filters)
  }

  private[akka] def unregisterClientFilters(remoteAddress: Address): Unit = filtersLock.withWriteGuard {
    filtersMap.remove(remoteAddress)
  }

  protected[akka] def createClient(address: InetSocketAddress, loader: Option[ClassLoader]): Client

  private def updateFilters(client: Client, address: Address) = {
    filtersMap.get(address) match {
      case Some(filters) => client.registerPipelines(filters)
      case None => client.unregisterPipelines
    }
  }
}
