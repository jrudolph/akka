/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.remoteinterface

import akka.japi.Creator
import java.net.InetSocketAddress
import akka.actor._
import akka.util._
import akka.dispatch.CompletableFuture
import akka.config.Config.{config, TIME_UNIT}
import java.util.concurrent.ConcurrentHashMap
import akka.AkkaException
import reflect.BeanProperty
import java.util.concurrent.atomic.AtomicReference

trait RemoteModule {
  val UUID_PREFIX        = "uuid:"
  type Filter = (Any => Any)
  
  def optimizeLocalScoped_?(): Boolean //Apply optimizations for remote operations in local scope
  protected[akka] def notifyListeners(message: => Any): Unit


  private[akka] def actors: ConcurrentHashMap[String, ActorRef]
  private[akka] def actorsByUuid: ConcurrentHashMap[String, ActorRef]
  private[akka] def actorsFactories: ConcurrentHashMap[String, () => ActorRef]
  private[akka] def typedActors: ConcurrentHashMap[String, AnyRef]
  private[akka] def typedActorsByUuid: ConcurrentHashMap[String, AnyRef]
  private[akka] def typedActorsFactories: ConcurrentHashMap[String, () => AnyRef]


  /** Lookup methods **/

  private[akka] def findActorById(id: String) : ActorRef = actors.get(id)

  private[akka] def findActorByUuid(uuid: String) : ActorRef = actorsByUuid.get(uuid)

  private[akka] def findActorFactory(id: String) : () => ActorRef = actorsFactories.get(id)

  private[akka] def findTypedActorById(id: String) : AnyRef = typedActors.get(id)

  private[akka] def findTypedActorFactory(id: String) : () => AnyRef = typedActorsFactories.get(id)

  private[akka] def findTypedActorByUuid(uuid: String) : AnyRef = typedActorsByUuid.get(uuid)

  private[akka] def findActorByIdOrUuid(id: String, uuid: String) : ActorRef = {
    var actorRefOrNull = if (id.startsWith(UUID_PREFIX)) findActorByUuid(id.substring(UUID_PREFIX.length))
                         else findActorById(id)
    if (actorRefOrNull eq null) actorRefOrNull = findActorByUuid(uuid)
    actorRefOrNull
  }

  private[akka] def findTypedActorByIdOrUuid(id: String, uuid: String) : AnyRef = {
    var actorRefOrNull = if (id.startsWith(UUID_PREFIX)) findTypedActorByUuid(id.substring(UUID_PREFIX.length))
                         else findTypedActorById(id)
    if (actorRefOrNull eq null) actorRefOrNull = findTypedActorByUuid(uuid)
    actorRefOrNull
  }
}

/**
 * Life-cycle events for RemoteClient.
 */
sealed trait RemoteClientLifeCycleEvent
case class RemoteClientError(
  @BeanProperty cause: Throwable,
  @BeanProperty client: RemoteClientModule,
  @BeanProperty remoteAddress: InetSocketAddress) extends RemoteClientLifeCycleEvent
case class RemoteClientDisconnected(
  @BeanProperty client: RemoteClientModule,
  @BeanProperty remoteAddress: InetSocketAddress) extends RemoteClientLifeCycleEvent
case class RemoteClientConnected(
  @BeanProperty client: RemoteClientModule,
  @BeanProperty remoteAddress: InetSocketAddress) extends RemoteClientLifeCycleEvent
case class RemoteClientStarted(
  @BeanProperty client: RemoteClientModule,
  @BeanProperty remoteAddress: InetSocketAddress) extends RemoteClientLifeCycleEvent
case class RemoteClientShutdown(
  @BeanProperty client: RemoteClientModule,
  @BeanProperty remoteAddress: InetSocketAddress) extends RemoteClientLifeCycleEvent
case class RemoteClientWriteFailed(
  @BeanProperty request: AnyRef,
  @BeanProperty cause: Throwable,
  @BeanProperty client: RemoteClientModule,
  @BeanProperty remoteAddress: InetSocketAddress) extends RemoteClientLifeCycleEvent


/**
 *  Life-cycle events for RemoteServer.
 */
sealed trait RemoteServerLifeCycleEvent
case class RemoteServerStarted(
  @BeanProperty val server: RemoteServerModule) extends RemoteServerLifeCycleEvent
case class RemoteServerShutdown(
  @BeanProperty val server: RemoteServerModule) extends RemoteServerLifeCycleEvent
case class RemoteServerError(
  @BeanProperty val cause: Throwable,
  @BeanProperty val server: RemoteServerModule) extends RemoteServerLifeCycleEvent
case class RemoteServerClientConnected(
  @BeanProperty val server: RemoteServerModule,
  @BeanProperty val clientAddress: Option[InetSocketAddress]) extends RemoteServerLifeCycleEvent
case class RemoteServerClientDisconnected(
  @BeanProperty val server: RemoteServerModule,
  @BeanProperty val clientAddress: Option[InetSocketAddress]) extends RemoteServerLifeCycleEvent
case class RemoteServerClientClosed(
  @BeanProperty val server: RemoteServerModule,
  @BeanProperty val clientAddress: Option[InetSocketAddress]) extends RemoteServerLifeCycleEvent
case class RemoteServerWriteFailed(
  @BeanProperty request: AnyRef,
  @BeanProperty cause: Throwable,
  @BeanProperty server: RemoteServerModule,
  @BeanProperty clientAddress: Option[InetSocketAddress]) extends RemoteServerLifeCycleEvent

/**
 * Thrown for example when trying to send a message using a RemoteClient that is either not started or shut down.
 */
class RemoteClientException private[akka] (message: String,
                                           @BeanProperty val client: RemoteClientModule,
                                           val remoteAddress: InetSocketAddress) extends AkkaException(message)

/**
 * Returned when a remote exception cannot be instantiated or parsed
 */
case class UnparsableException private[akka] (originalClassName: String,
                                              originalMessage: String) extends AkkaException(originalMessage)


abstract class RemoteSupport extends ListenerManagement with RemoteServerModule with RemoteClientModule {
  def shutdown {
    this.shutdownClientModule
    this.shutdownServerModule
    clear
  }


  /**
   * Creates a Client-managed ActorRef out of the Actor of the specified Class.
   * If the supplied host and port is identical of the configured local node, it will be a local actor
   * <pre>
   *   import Actor._
   *   val actor = actorOf(classOf[MyActor],"www.akka.io", 2552)
   *   actor.start
   *   actor ! message
   *   actor.stop
   * </pre>
   * You can create and start the actor in one statement like this:
   * <pre>
   *   val actor = actorOf(classOf[MyActor],"www.akka.io", 2552).start
   * </pre>
   */
  @deprecated("Will be removed after 1.1")
  def actorOf(factory: => Actor, host: String, port: Int): ActorRef =
    Actor.remote.clientManagedActorOf(() => factory, host, port)

  /**
   * Creates a Client-managed ActorRef out of the Actor of the specified Class.
   * If the supplied host and port is identical of the configured local node, it will be a local actor
   * <pre>
   *   import Actor._
   *   val actor = actorOf(classOf[MyActor],"www.akka.io",2552)
   *   actor.start
   *   actor ! message
   *   actor.stop
   * </pre>
   * You can create and start the actor in one statement like this:
   * <pre>
   *   val actor = actorOf(classOf[MyActor],"www.akka.io",2552).start
   * </pre>
   */
  @deprecated("Will be removed after 1.1")
  def actorOf(clazz: Class[_ <: Actor], host: String, port: Int): ActorRef = {
    import ReflectiveAccess.{ createInstance, noParams, noArgs }
    clientManagedActorOf(() =>
        createInstance[Actor](clazz.asInstanceOf[Class[_]], noParams, noArgs).getOrElse(
          throw new ActorInitializationException(
            "Could not instantiate Actor" +
            "\nMake sure Actor is NOT defined inside a class/trait," +
            "\nif so put it outside the class/trait, f.e. in a companion object," +
            "\nOR try to change: 'actorOf[MyActor]' to 'actorOf(new MyActor)'.")),
      host, port)
  }

  /**
   * Creates a Client-managed ActorRef out of the Actor of the specified Class.
   * If the supplied host and port is identical of the configured local node, it will be a local actor
   * <pre>
   *   import Actor._
   *   val actor = actorOf[MyActor]("www.akka.io",2552)
   *   actor.start
   *   actor ! message
   *   actor.stop
   * </pre>
   * You can create and start the actor in one statement like this:
   * <pre>
   *   val actor = actorOf[MyActor]("www.akka.io",2552).start
   * </pre>
   */
  @deprecated("Will be removed after 1.1")
  def actorOf[T <: Actor : Manifest](host: String, port: Int): ActorRef = {
    import ReflectiveAccess.{ createInstance, noParams, noArgs }
    clientManagedActorOf(() =>
      createInstance[Actor](manifest[T].erasure.asInstanceOf[Class[_]], noParams, noArgs).getOrElse(
        throw new ActorInitializationException(
          "Could not instantiate Actor" +
          "\nMake sure Actor is NOT defined inside a class/trait," +
          "\nif so put it outside the class/trait, f.e. in a companion object," +
          "\nOR try to change: 'actorOf[MyActor]' to 'actorOf(new MyActor)'.")),
      host, port)
  }

  protected override def manageLifeCycleOfListeners = false
  protected[akka] override def notifyListeners(message: => Any): Unit = super.notifyListeners(message)

  private[akka] val actors               = new ConcurrentHashMap[String, ActorRef]
  private[akka] val actorsByUuid         = new ConcurrentHashMap[String, ActorRef]
  private[akka] val actorsFactories      = new ConcurrentHashMap[String, () => ActorRef]
  private[akka] val typedActors          = new ConcurrentHashMap[String, AnyRef]
  private[akka] val typedActorsByUuid    = new ConcurrentHashMap[String, AnyRef]
  private[akka] val typedActorsFactories = new ConcurrentHashMap[String, () => AnyRef]

  def clear {
    actors.clear
    actorsByUuid.clear
    typedActors.clear
    typedActorsByUuid.clear
    actorsFactories.clear
    typedActorsFactories.clear
  }
}

/**
 * This is the interface for the RemoteServer functionality, it's used in Actor.remote
 */
trait RemoteServerModule extends RemoteModule {
  protected val guard = new ReentrantGuard

  /**
   * Signals whether the server is up and running or not
   */
  def isRunning: Boolean

  /**
   *  Gets the name of the server instance
   */
  def name: String

  /**
   * Gets the address of the server instance
   */
  def address: InetSocketAddress

  /**
   *  Starts the server up
   */
  def start(): RemoteServerModule =
    start(ReflectiveAccess.Remote.configDefaultAddress.getAddress.getHostAddress,
          ReflectiveAccess.Remote.configDefaultAddress.getPort,
          None)

  /**
   *  Starts the server up
   */
  def start(loader: ClassLoader): RemoteServerModule =
    start(ReflectiveAccess.Remote.configDefaultAddress.getAddress.getHostAddress,
          ReflectiveAccess.Remote.configDefaultAddress.getPort,
          Option(loader))

  /**
   *  Starts the server up
   */
  def start(host: String, port: Int): RemoteServerModule =
    start(host,port,None)

  /**
   *  Starts the server up
   */
  def start(host: String, port: Int, loader: ClassLoader): RemoteServerModule =
    start(host,port,Option(loader))

  /**
   *  Starts the server up
   */
  def start(host: String, port: Int, loader: Option[ClassLoader]): RemoteServerModule

  /**
   *  Shuts the server down
   */
  def shutdownServerModule(): Unit

  /**
   *  Register typed actor by interface name.
   */
  def registerTypedActor(intfClass: Class[_], typedActor: AnyRef) : Unit = registerTypedActor(intfClass.getName, typedActor)

  /**
   * Register remote typed actor by a specific id.
   * @param id custom actor id
   * @param typedActor typed actor to register
   */
  def registerTypedActor(id: String, typedActor: AnyRef): Unit

  /**
   * Register typed actor by interface name.
   */
  def registerTypedPerSessionActor(intfClass: Class[_], factory: => AnyRef) : Unit = registerTypedActor(intfClass.getName, factory)

  /**
   * Register typed actor by interface name.
   * Java API
   */
  def registerTypedPerSessionActor(intfClass: Class[_], factory: Creator[AnyRef]) : Unit = registerTypedActor(intfClass.getName, factory)

  /**
   * Register remote typed actor by a specific id.
   * @param id custom actor id
   * @param typedActor typed actor to register
   */
  def registerTypedPerSessionActor(id: String, factory: => AnyRef): Unit

  /**
   * Register remote typed actor by a specific id.
   * @param id custom actor id
   * @param typedActor typed actor to register
   * Java API
   */
  def registerTypedPerSessionActor(id: String, factory: Creator[AnyRef]): Unit = registerTypedPerSessionActor(id, factory.create)

  /**
   * Register Remote Actor by the Actor's 'id' field. It starts the Actor if it is not started already.
   */
  def register(actorRef: ActorRef): Unit = register(actorRef.id, actorRef)

  /**
   *  Register Remote Actor by the Actor's uuid field. It starts the Actor if it is not started already.
   */
  def registerByUuid(actorRef: ActorRef): Unit

  /**
   *  Register Remote Actor by a specific 'id' passed as argument.
   * <p/>
   * NOTE: If you use this method to register your remote actor then you must unregister the actor by this ID yourself.
   */
  def register(id: String, actorRef: ActorRef): Unit

  /**
   * Register Remote Session Actor by a specific 'id' passed as argument.
   * <p/>
   * NOTE: If you use this method to register your remote actor then you must unregister the actor by this ID yourself.
   */
  def registerPerSession(id: String, factory: => ActorRef): Unit

  /**
   * Register Remote Session Actor by a specific 'id' passed as argument.
   * <p/>
   * NOTE: If you use this method to register your remote actor then you must unregister the actor by this ID yourself.
   * Java API
   */
  def registerPerSession(id: String, factory: Creator[ActorRef]): Unit = registerPerSession(id, factory.create)

  /**
   * Unregister Remote Actor that is registered using its 'id' field (not custom ID).
   */
  def unregister(actorRef: ActorRef): Unit

  /**
   * Unregister Remote Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregister(id: String): Unit

  /**
   * Unregister Remote Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregisterPerSession(id: String): Unit

  /**
   * Unregister Remote Typed Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregisterTypedActor(id: String): Unit

  /**
   * Unregister Remote Typed Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregisterTypedPerSessionActor(id: String): Unit

  /**
   * Returns the serverFilters on the remote server endpoint. Incoming and outgoing messages to and from the remote server can be intercepted
   * and modified with pipelines of filter functions.
   * <pre>
   *   // create some functions that match on RemoteMessageProtocol.Builder and return a RemoteMessageProtocol.Builder or None
   *   // if the function wants to abort the chain and stop the message from passing through. for instance:
   *   val function1: (Any=>Any) = {
   *     case builder:RemoteMessageProtocol.Builder => {
   *      // do something with builder and return builder, or None to abort chain
   *      builder
   *     }
   *     case _ => None
   *   }
   *   // create some more functions
   *   ...
   *   val incomingFilters = Vector(function1, function2, ...)
   *   // add a vector of filters (function1, 2, ...) when the server receives a remoteprotocol message
   *   remote.serverFilters.in(incomingFilters)
   *   // add a vector of filters when the server sends back a remoteprotocol message
   *   remote.serverFilters.out(outgoingFilters)
   *   // you can remove all incoming and outgoing filters with clear
   *   remote.serverFilters.clear()
   *   // you can chain the calls
   *   remote.serverFilters.in(incomingFilters).out(outgoingFilters)
   * </pre>
   * <p/>
   * all modifications done on the serverFilters using the Filters trait are immediately active on the current remote server that is running.
   * If the remote server is not running yet (start has not been called), the serverFilters will become active when the server is started.
   * The serverFilters stay available after server shutdown and will continue to be active after a server start, even if the server is started
   * on a different address.
   */
  def serverFilters: Filters

}

trait RemoteClientModule extends RemoteModule { self: RemoteModule =>

  def actorFor(classNameOrServiceId: String, hostname: String, port: Int): ActorRef =
    actorFor(classNameOrServiceId, classNameOrServiceId, Actor.TIMEOUT, hostname, port, None)

  def actorFor(classNameOrServiceId: String, hostname: String, port: Int, loader: ClassLoader): ActorRef =
    actorFor(classNameOrServiceId, classNameOrServiceId, Actor.TIMEOUT, hostname, port, Some(loader))

  def actorFor(serviceId: String, className: String, hostname: String, port: Int): ActorRef =
    actorFor(serviceId, className, Actor.TIMEOUT, hostname, port, None)

  def actorFor(serviceId: String, className: String, hostname: String, port: Int, loader: ClassLoader): ActorRef =
    actorFor(serviceId, className, Actor.TIMEOUT, hostname, port, Some(loader))

  def actorFor(classNameOrServiceId: String, timeout: Long, hostname: String, port: Int): ActorRef =
    actorFor(classNameOrServiceId, classNameOrServiceId, timeout, hostname, port, None)

  def actorFor(classNameOrServiceId: String, timeout: Long, hostname: String, port: Int, loader: ClassLoader): ActorRef =
    actorFor(classNameOrServiceId, classNameOrServiceId, timeout, hostname, port, Some(loader))

  def actorFor(serviceId: String, className: String, timeout: Long, hostname: String, port: Int): ActorRef =
    actorFor(serviceId, className, timeout, hostname, port, None)

  def typedActorFor[T](intfClass: Class[T], serviceIdOrClassName: String, hostname: String, port: Int): T =
    typedActorFor(intfClass, serviceIdOrClassName, serviceIdOrClassName, Actor.TIMEOUT, hostname, port, None)

  def typedActorFor[T](intfClass: Class[T], serviceIdOrClassName: String, timeout: Long, hostname: String, port: Int): T =
    typedActorFor(intfClass, serviceIdOrClassName, serviceIdOrClassName, timeout, hostname, port, None)

  def typedActorFor[T](intfClass: Class[T], serviceIdOrClassName: String, timeout: Long, hostname: String, port: Int, loader: ClassLoader): T =
    typedActorFor(intfClass, serviceIdOrClassName, serviceIdOrClassName, timeout, hostname, port, Some(loader))

  def typedActorFor[T](intfClass: Class[T], serviceId: String, implClassName: String, timeout: Long, hostname: String, port: Int, loader: ClassLoader): T =
    typedActorFor(intfClass, serviceId, implClassName, timeout, hostname, port, Some(loader))

  @deprecated("Will be removed after 1.1")
  def clientManagedActorOf(factory: () => Actor, host: String, port: Int): ActorRef


  /**
   * Clean-up all open connections.
   */
  def shutdownClientModule(): Unit

  /**
   * Shuts down a specific client connected to the supplied remote address returns true if successful
   */
  def shutdownClientConnection(address: InetSocketAddress): Boolean

  /**
   * Restarts a specific client connected to the supplied remote address, but only if the client is not shut down
   */
  def restartClientConnection(address: InetSocketAddress): Boolean

  /**
   * Access the filters on the client endpoint sending to and receiving from the address (hostname, port).
   * Filters provides an interface to register the incoming and outgoing filters or to clear all filters
   * registered on the client endpoint for the address (hostname, port).
   *
   * Incoming and outgoing messages to and from the remote client can be intercepted
   * and modified with pipelines of filter functions.
   * <pre>
   *   // create some functions that match on RemoteMessageProtocol.Builder and return a RemoteMessageProtocol.Builder or None
   *   // if the function wants to abort the chain and stop the message from passing through. for instance:
   *   val function1: (Any=>Any) = {
   *     case builder:RemoteMessageProtocol.Builder => {
   *      // do something with builder and return builder, or None to abort chain
   *      builder
   *     }
   *     case _ => None
   *   }
   *   // create some more functions
   *   ...
   *   val incomingFilters = Vector(function1, function2, ...)
   *   // add a vector of filters (function1, 2, ...) when the client receives a remoteprotocol message
   *   remote.clientFilters("server", 25220).in(incomingFilters)
   *   // add a vector of filters when the client sends a remoteprotocol message
   *   remote.clientFilters("server", 25220).out(outgoingFilters)
   *   // you can remove all incoming and outgoing filters with clear
   *   remote.clientFilters("server", 25220).clear()
   *   // you can chain the calls
   *   remote.clientFilters("server", 25220).in(incomingFilters).out(outgoingFilters)
   * </pre>
   * <p/>
   * all modifications done on the clientFilters using the Filters trait are immediately active on the remote client for the address (hostname, port).
   * If the remote client is not running yet, the clientFilters will become active when the remote client is used.
   * The clientFilters stay available after client shutdown for the same address (hostname, port).
   *
   * @param hostname the hostname
   * @param port the port
   * @returns the client filters for the address
   */
  def clientFilters(hostname: String, port:Int): Filters

  /** Methods that needs to be implemented by a transport **/

  protected[akka] def typedActorFor[T](intfClass: Class[T], serviceId: String, implClassName: String, timeout: Long, host: String, port: Int, loader: Option[ClassLoader]): T

  protected[akka] def actorFor(serviceId: String, className: String, timeout: Long, hostname: String, port: Int, loader: Option[ClassLoader]): ActorRef

  protected[akka] def send[T](message: Any,
                              senderOption: Option[ActorRef],
                              senderFuture: Option[CompletableFuture[T]],
                              remoteAddress: InetSocketAddress,
                              timeout: Long,
                              isOneWay: Boolean,
                              actorRef: ActorRef,
                              typedActorInfo: Option[Tuple2[String, String]],
                              actorType: ActorType,
                              loader: Option[ClassLoader]): Option[CompletableFuture[T]]

  private[akka] def registerSupervisorForActor(actorRef: ActorRef): ActorRef

  private[akka] def deregisterSupervisorForActor(actorRef: ActorRef): ActorRef

  @deprecated("Will be removed after 1.1")
  private[akka] def registerClientManagedActor(hostname: String, port: Int, uuid: Uuid): Unit

  @deprecated("Will be removed after 1.1")
  private[akka] def unregisterClientManagedActor(hostname: String, port: Int, uuid: Uuid): Unit
}

/**
 * Abstraction for the filters that can be attached on a remote actor communication endpoint.
 * Mini fluent interface to the client and server Filters, so that you can do
 * filters.in(incoming).out(outgoing) or filters.in(incoming), filters.clear.
 * This trait is used in remote.clientFilters and remote.serverFilters.
 * The filters can be added to the remote side endpoint and/or the server side endpoint.
 * <p/>
 * On both endpoints vectors of filters can be placed on the incoming and outgoing messages.
 * The vector of filter functions is also referred to as a pipeline. Per endpoint there are two pipelines, one incoming, one outgoing.
 * For the complete communication between a remote client and server, there are four pipelines, client outgoing, client incoming,
 * server incoming, server outgoing.
 * <p/>
 * The Filter function is a <pre> Any=>Any </pre> function which will receive a RemoteMessageProtocol.Builder and will need to return a
 * RemoteMessageProtocol.Builder, or None if the Filter function wants to abort the chain of functions and stop the message from being sent/received.
 * The builder can be modified in the filter and passed on to the next function in the vector by using the standard protobuf methods.
 * This gives the opportunity to add metadata to the message or filter out certain messages or just listen in on what messages are being
 * sent between remote client and server.
 */
trait Filters {
  protected val guard = new ReadWriteGuard
  type Filter = (Any => Any)
  protected val _in  = new AtomicReference[Option[Vector[Filter]]](None)
  protected val _out = new AtomicReference[Option[Vector[Filter]]](None)
  /**
   * should be used to update the state of the filters in the server or client module
   */
  private [akka] def update():Unit
  /**
   * should be used to clear the state of the filters in the server or client module
   */
  private [akka] def unregister():Unit

  /**
   * Adds a pipeline (vector of Filter functions) to the incoming/receiving side of the endpoint.
   * Whenever an endpoint receives a message, it is first passed through the vector of filters and then passed on to
   * the normal handling of the message at the endpoint.
   */
  def in(filters: Vector[Filter]): Filters =  guard withWriteGuard {
    _in.set(Some(filters))
    update()
    this
  }

  /**
   * Adds a pipeline (vector of Filter functions) to the outgoing/sending side of the filters.
   * Whenever an endpoint sends a message, it is first passed through the vector of filters and then passed on to
   * the normal handling of the message at the endpoint.
   */
  def out(filters: Vector[Filter]): Filters = guard withWriteGuard {
    _out.set(Some(filters))
    update()
    this
  }

  /**
   * Clears both the incoming and outgoing side of the filters.
   */
  def clear: Filters = guard withWriteGuard {
    _in.set(None)
    _out.set(None)
    unregister()
    this
  }
}