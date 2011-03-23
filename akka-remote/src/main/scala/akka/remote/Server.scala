/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.remote

import java.util.concurrent.ConcurrentHashMap
import java.net.InetSocketAddress
import akka.serialization.RemoteActorSerialization
import java.lang.reflect.InvocationTargetException
import akka.dispatch.{Future, DefaultCompletableFuture}
import akka.event.EventHandler
import akka.actor.{ TypedActor, Actor, LifeCycleMessage, RemoteActorSystemMessage, IllegalActorStateException, ActorRef, ActorType => AkkaActorType, uuidFrom, Uuid}
import akka.remote.protocol.RemoteProtocol._
import akka.remote.protocol.RemoteProtocol.ActorType._
import akka.remoteinterface._
import java.util.concurrent.atomic._
import akka.util._

/**
 * remote server interface that needs to be implemented by a specific transport
 */
trait RemoteServer {
  def name: String

  def host: String

  def port: Int

  def address: InetSocketAddress

  def shutdown

  protected[akka] def registerPipelines(pipelines: Pipelines): Unit

  protected[akka] def unregisterPipelines(): Unit

}

/**
 * ServerFilters, used from remote.serverFilters. updates itself on the server through a ServerFilterRegistry
 */
private[akka] class ServerFilters(serverFilterRegistry: ServerFilterRegistry) extends Filters with Pipelines {

  private[akka] def update() = {
    serverFilterRegistry.registerServerFilters(this)
  }

  private[akka] def unregister() = {
    serverFilterRegistry.unregisterServerFilters()
  }
}

/**
 * an interface to the server module to register and unregister filters
 */
private [akka] trait ServerFilterRegistry {
  private[akka] def registerServerFilters(filters: ServerFilters): Unit

  private[akka] def unregisterServerFilters(): Unit
}

/**
 * Default base remote server module that can be extended to implement a RemoteServerModule
 */
trait DefaultRemoteServerModule extends RemoteServerModule with ServerFilterRegistry {
  self: RemoteModule =>

  import RemoteServerSettings._

  private[akka] val currentServer = new AtomicReference[Option[RemoteServer]](None)
  private[akka] val currentFilters = new AtomicReference[Option[ServerFilters]](None)

  def address = currentServer.get match {
    case s: Some[RemoteServer] => s.get.address
    case None => ReflectiveAccess.Remote.configDefaultAddress
  }

  def name = currentServer.get match {
    case s: Some[RemoteServer] => s.get.name
    case None =>
      val transport = ReflectiveAccess.Remote.TRANSPORT
      val transportName = transport.split('.').last
      val a = ReflectiveAccess.Remote.configDefaultAddress
      transportName + "@" + a.getHostName + ":" + a.getPort
  }

  private val _isRunning = new Switch(false)

  def isRunning = _isRunning.isOn

  def createRemoteServer(remoteServerModule: RemoteServerModule, hostname: String, port: Int, loader: Option[ClassLoader]): RemoteServer

  def start(_hostname: String, _port: Int, loader: Option[ClassLoader] = None): RemoteServerModule = guard withGuard {
    try {
      _isRunning switchOn {
        val server = createRemoteServer(this, _hostname, _port, loader)
        currentFilters.get.foreach {
          server.registerPipelines(_)
        }
        currentServer.set(Some(server))
      }
    } catch {
      case e: Exception =>
        EventHandler.error(e, this, e.getMessage)
        notifyListeners(RemoteServerError(e, this))
    }
    this
  }

  def shutdownServerModule = guard withGuard {
    _isRunning switchOff {
      currentServer.getAndSet(None) foreach {
        instance =>
          instance.shutdown
      }
    }
  }

  /**
   * Register remote typed actor by a specific id.
   * @param id custom actor id
   * @param typedActor typed actor to register
   */
  def registerTypedActor(id: String, typedActor: AnyRef): Unit = guard withGuard {
    if (id.startsWith(UUID_PREFIX)) registerTypedActor(id.substring(UUID_PREFIX.length), typedActor, typedActorsByUuid)
    else registerTypedActor(id, typedActor, typedActors)
  }

  /**
   * Register remote typed actor by a specific id.
   * @param id custom actor id
   * @param typedActor typed actor to register
   */
  def registerTypedPerSessionActor(id: String, factory: => AnyRef): Unit = guard withGuard {
    registerTypedPerSessionActor(id, () => factory, typedActorsFactories)
  }

  /**
   * Register Remote Actor by a specific 'id' passed as argument.
   * <p/>
   * NOTE: If you use this method to register your remote actor then you must unregister the actor by this ID yourself.
   */
  def register(id: String, actorRef: ActorRef): Unit = guard withGuard {
    if (id.startsWith(UUID_PREFIX)) register(id.substring(UUID_PREFIX.length), actorRef, actorsByUuid)
    else register(id, actorRef, actors)
  }

  def registerByUuid(actorRef: ActorRef): Unit = guard withGuard {
    register(actorRef.uuid.toString, actorRef, actorsByUuid)
  }

  private def register[Key](id: Key, actorRef: ActorRef, registry: ConcurrentHashMap[Key, ActorRef]) {
    if (_isRunning.isOn) {
      registry.put(id, actorRef) //TODO change to putIfAbsent
      if (!actorRef.isRunning) actorRef.start
    }
  }

  /**
   * Register Remote Session Actor by a specific 'id' passed as argument.
   * <p/>
   * NOTE: If you use this method to register your remote actor then you must unregister the actor by this ID yourself.
   */
  def registerPerSession(id: String, factory: => ActorRef): Unit = synchronized {
    registerPerSession(id, () => factory, actorsFactories)
  }

  private def registerPerSession[Key](id: Key, factory: () => ActorRef, registry: ConcurrentHashMap[Key, () => ActorRef]) {
    if (_isRunning.isOn)
      registry.put(id, factory) //TODO change to putIfAbsent
  }

  private def registerTypedActor[Key](id: Key, typedActor: AnyRef, registry: ConcurrentHashMap[Key, AnyRef]) {
    if (_isRunning.isOn)
      registry.put(id, typedActor) //TODO change to putIfAbsent
  }

  private def registerTypedPerSessionActor[Key](id: Key, factory: () => AnyRef, registry: ConcurrentHashMap[Key, () => AnyRef]) {
    if (_isRunning.isOn)
      registry.put(id, factory) //TODO change to putIfAbsent
  }

  /**
   * Unregister Remote Actor that is registered using its 'id' field (not custom ID).
   */
  def unregister(actorRef: ActorRef): Unit = guard withGuard {
    if (_isRunning.isOn) {
      actors.remove(actorRef.id, actorRef)
      actorsByUuid.remove(actorRef.uuid, actorRef)
    }
  }

  /**
   * Unregister Remote Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregister(id: String): Unit = guard withGuard {
    if (_isRunning.isOn) {
      if (id.startsWith(UUID_PREFIX)) actorsByUuid.remove(id.substring(UUID_PREFIX.length))
      else {
        val actorRef = actors get id
        actorsByUuid.remove(actorRef.uuid, actorRef)
        actors.remove(id, actorRef)
      }
    }
  }

  /**
   * Unregister Remote Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregisterPerSession(id: String): Unit = {
    if (_isRunning.isOn) {
      actorsFactories.remove(id)
    }
  }

  /**
   * Unregister Remote Typed Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregisterTypedActor(id: String): Unit = guard withGuard {
    if (_isRunning.isOn) {
      if (id.startsWith(UUID_PREFIX)) typedActorsByUuid.remove(id.substring(UUID_PREFIX.length))
      else typedActors.remove(id)
    }
  }

  /**
   * Unregister Remote Typed Actor by specific 'id'.
   * <p/>
   * NOTE: You need to call this method if you have registered an actor by a custom ID.
   */
  def unregisterTypedPerSessionActor(id: String): Unit =
    if (_isRunning.isOn) typedActorsFactories.remove(id)

  def serverFilters = guard withGuard {
    currentFilters.get match {
      case Some(filters) => {
        filters
      }
      case None => {
        val filters = new ServerFilters(this)
        currentFilters.set(Some(filters))
        filters
      }
    }
  }

  private[akka] def registerServerFilters(filters: ServerFilters): Unit = guard withGuard {
    currentFilters.set(Some(filters))
    if (_isRunning.isOn) {
      currentServer.get.foreach {
        server =>
          server.registerPipelines(filters)
      }
    }
  }

  private[akka] def unregisterServerFilters(): Unit = guard withGuard {
    if (_isRunning.isOn) {
      currentServer.get.foreach {
        server =>
          server.unregisterPipelines()
      }
    }
  }
}

/**
 * A wrapper around the channel so it can be abstracted and used from a more generic mixin
 */
trait Session[T <: Comparable[T]] {
  def write(payload: AkkaRemoteProtocol): Unit

  def channel: T
}

// handles remote Message protocol and dispatches to Actors, T is the Channel type key for sessions
trait RemoteMessageProtocolHandler[T <: Comparable[T]] {

  import RemoteServerSettings._

  //TODO set
  val applicationLoader: Option[ClassLoader]
  val server: RemoteServerModule
  val currentPipelines = new AtomicReference[Option[Pipelines]](None)

  def findSessionActor(id: String, channel: T): ActorRef

  def findTypedSessionActor(id: String, channel: T): AnyRef

  def getTypedSessionActors(channel: T): ConcurrentHashMap[String, AnyRef]

  def getSessionActors(channel: T): ConcurrentHashMap[String, ActorRef]

  applicationLoader.foreach(MessageSerializer.setClassLoader(_))

  //TODO: REVISIT: THIS FEELS A BIT DODGY

  def dispatch(request: RemoteMessageProtocol, session: Session[T]): Any = {
    //FIXME we should definitely spawn off this in a thread pool or something,
    //      potentially using Actor.spawn or something similar
    request.getActorInfo.getActorType match {
      case SCALA_ACTOR => dispatchToActor(request, session)
      case TYPED_ACTOR => dispatchToTypedActor(request, session)
      case JAVA_ACTOR => throw new IllegalActorStateException("ActorType JAVA_ACTOR is currently not supported")
      case other => throw new IllegalActorStateException("Unknown ActorType [" + other + "]")
    }
  }

  private[akka] def handleRemoteMessageProtocol(request: RemoteMessageProtocol, session: Session[T]) = {
    currentPipelines.get match {
      case Some(pipelines) => {
        if (pipelines.isIncomingEmpty) {
          dispatch(request, session)
        } else {
          pipelines.incoming(request.toBuilder) foreach {
            builder => dispatch(builder.build, session)
          }
        }
      }
      case None => dispatch(request, session)
    }
  }


  private[akka] def dispatchToActor(request: RemoteMessageProtocol, session: Session[T]) {
    val actorInfo = request.getActorInfo
    val actorRef =
      try {
        createActor(actorInfo, session.channel)
      } catch {
        case e: SecurityException =>
          EventHandler.error(e, this, e.getMessage)
          session.write(createErrorReplyMessage(e, request, AkkaActorType.ScalaActor))
          server.notifyListeners(RemoteServerError(e, server))
          return
      }

    val message = MessageSerializer.deserialize(request.getMessage)
    val sender =
      if (request.hasSender) Some(RemoteActorSerialization.fromProtobufToRemoteActorRef(request.getSender, applicationLoader))
      else None

    message match {
    // first match on system messages
      case RemoteActorSystemMessage.Stop =>
        if (UNTRUSTED_MODE) throw new SecurityException("Remote server is operating is untrusted mode, can not stop the actor")
        else actorRef.stop
      case _: LifeCycleMessage if (UNTRUSTED_MODE) =>
        throw new SecurityException("Remote server is operating is untrusted mode, can not pass on a LifeCycleMessage to the remote actor")

      case _ => // then match on user defined messages
        if (request.getOneWay) actorRef.!(message)(sender)
        else actorRef.postMessageToMailboxAndCreateFutureResultWithTimeout(
          message,
          request.getActorInfo.getTimeout,
          None,
          Some(new DefaultCompletableFuture[Any](request.getActorInfo.getTimeout).
              onComplete(_.value.get match {
            case l: Left[Throwable, Any] => session.write(createErrorReplyMessage(l.a, request, AkkaActorType.ScalaActor))
            case r: Right[Throwable, Any] =>
              val messageBuilder = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
                Some(actorRef),
                Right(request.getUuid),
                actorInfo.getId,
                actorInfo.getTarget,
                actorInfo.getTimeout,
                r,
                true,
                Some(actorRef),
                None,
                AkkaActorType.ScalaActor,
                None)

              // FIXME lift in the supervisor uuid management into toh createRemoteMessageProtocolBuilder method
              if (request.hasSupervisorUuid) messageBuilder.setSupervisorUuid(request.getSupervisorUuid)
              currentPipelines.get match {
                case Some(pipelines) => {
                  if (pipelines.isOutgoingEmpty) {
                    session.write(RemoteEncoder.encode(messageBuilder.build))
                  } else {
                    pipelines.outgoing(messageBuilder) foreach {
                      builder => session.write(RemoteEncoder.encode(builder.build))
                    }
                  }
                }
                case None => session.write(RemoteEncoder.encode(messageBuilder.build))
              }
            }
          ))
        )
    }
  }

  private[akka] def dispatchToTypedActor(request: RemoteMessageProtocol, session: Session[T]) = {
    val actorInfo = request.getActorInfo
    val typedActorInfo = actorInfo.getTypedActorInfo

    val typedActor = createTypedActor(actorInfo, session.channel)
    //FIXME: Add ownerTypeHint and parameter types to the TypedActorInfo?
    val (ownerTypeHint, argClasses, args) = MessageSerializer.deserialize(request.getMessage).asInstanceOf[Tuple3[String,Array[Class[_]],Array[AnyRef]]]

    def resolveMethod(bottomType: Class[_], typeHint: String, methodName: String, methodSignature: Array[Class[_]]): java.lang.reflect.Method = {
      var typeToResolve = bottomType
      var targetMethod: java.lang.reflect.Method = null
      var firstException: NoSuchMethodException = null
      while((typeToResolve ne null) && (targetMethod eq null)) {

        if ((typeHint eq null) || typeToResolve.getName.startsWith(typeHint)) {
          try {
            targetMethod = typeToResolve.getDeclaredMethod(methodName, methodSignature:_*)
            targetMethod.setAccessible(true)
          } catch {
            case e: NoSuchMethodException =>
              if (firstException eq null)
                firstException = e

          }
        }

        typeToResolve = typeToResolve.getSuperclass
      }

      if((targetMethod eq null) && (firstException ne null))
        throw firstException

      targetMethod
    }

    try {
      val messageReceiver = resolveMethod(typedActor.getClass, ownerTypeHint, typedActorInfo.getMethod, argClasses)

      if (request.getOneWay) messageReceiver.invoke(typedActor, args: _*) //FIXME execute in non-IO thread
      else {
        //Sends the response
        def sendResponse(result: Either[Throwable,Any]): Unit = try {
          val messageBuilder = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
            None,
            Right(request.getUuid),
            actorInfo.getId,
            actorInfo.getTarget,
            actorInfo.getTimeout,
            result,
            true,
            None,
            None,
            AkkaActorType.TypedActor,
            None)
          if (request.hasSupervisorUuid) messageBuilder.setSupervisorUuid(request.getSupervisorUuid)

          session.write(RemoteEncoder.encode(messageBuilder.build))
        } catch {
          case e: Exception =>
            EventHandler.error(e, this, e.getMessage)
            server.notifyListeners(RemoteServerError(e, server))
        }

        messageReceiver.invoke(typedActor, args: _*) match { //FIXME execute in non-IO thread
          //If it's a future, we can lift on that to defer the send to when the future is completed
          case f: Future[_] => f.onComplete( future => sendResponse(future.value.get) )
          case other        => sendResponse(Right(other))
        }
      }
    } catch {
      case e: InvocationTargetException =>
        EventHandler.error(e, this, e.getMessage)
        session.write(createErrorReplyMessage(e.getCause, request, AkkaActorType.TypedActor))
        server.notifyListeners(RemoteServerError(e, server))
      case e: Exception =>
        EventHandler.error(e, this, e.getMessage)
        session.write(createErrorReplyMessage(e, request, AkkaActorType.TypedActor))
        server.notifyListeners(RemoteServerError(e, server))
    }
  }


  /**
   * gets the actor from the session, or creates one if there is a factory for it
   */
  private def createSessionActor(actorInfo: ActorInfoProtocol, channel:T): ActorRef = {
    val uuid = actorInfo.getUuid
    val id = actorInfo.getId

    findSessionActor(id, channel) match {
      case null => // we dont have it in the session either, see if we have a factory for it
        server.findActorFactory(id) match {
          case null => null
          case factory =>
            val actorRef = factory()
            actorRef.uuid = parseUuid(uuid) //FIXME is this sensible?
            getSessionActors(channel).put(id, actorRef)
            actorRef.start //Start it where's it's created
        }
      case sessionActor => sessionActor
    }
  }

  /**
   * Creates a client managed actor and starts the actor
   */
  private def createClientManagedActor(actorInfo: ActorInfoProtocol): ActorRef = {
    val uuid = actorInfo.getUuid
    val id = actorInfo.getId
    val timeout = actorInfo.getTimeout
    val name = actorInfo.getTarget

    try {
      if (UNTRUSTED_MODE) throw new SecurityException(
        "Remote server is operating is untrusted mode, can not create remote actors on behalf of the remote client")

      val clazz = if (applicationLoader.isDefined) applicationLoader.get.loadClass(name)
                  else Class.forName(name)
      val actorRef = Actor.actorOf(clazz.asInstanceOf[Class[_ <: Actor]])
      actorRef.uuid = parseUuid(uuid)
      actorRef.id = id
      actorRef.timeout = timeout
      server.actorsByUuid.put(actorRef.uuid.toString, actorRef) // register by uuid
      actorRef.start //Start it where it's created
    } catch {
      case e: Throwable =>
        EventHandler.error(e, this, e.getMessage)
        server.notifyListeners(RemoteServerError(e, server))
        throw e
    }

  }

  /**
   * Creates a new instance of the actor with name, uuid and timeout specified as arguments.
   *
   * If actor already created then just return it from the registry.
   *
   * Does not start the actor.
   */
  private def createActor(actorInfo: ActorInfoProtocol, channel: T): ActorRef = {
    val uuid = actorInfo.getUuid
    val id = actorInfo.getId

    server.findActorByIdOrUuid(id, parseUuid(uuid).toString) match {
      case null => // the actor has not been registered globally. See if we have it in the session
        createSessionActor(actorInfo, channel) match {
          case null => createClientManagedActor(actorInfo) // maybe it is a client managed actor
          case sessionActor => sessionActor
        }
      case actorRef => actorRef
    }
  }

  /**
   * gets the actor from the session, or creates one if there is a factory for it
   */
  private def createTypedSessionActor(actorInfo: ActorInfoProtocol, channel: T):AnyRef ={
    val id = actorInfo.getId
    findTypedSessionActor(id, channel) match {
      case null =>
        server.findTypedActorFactory(id) match {
          case null => null
          case factory =>
            val newInstance = factory()
            getTypedSessionActors(channel).put(id, newInstance)
            newInstance
        }
      case sessionActor => sessionActor
    }
  }

  private def createClientManagedTypedActor(actorInfo: ActorInfoProtocol) = {
    val typedActorInfo = actorInfo.getTypedActorInfo
    val interfaceClassname = typedActorInfo.getInterface
    val targetClassname = actorInfo.getTarget
    val uuid = actorInfo.getUuid

    try {
      if (UNTRUSTED_MODE) throw new SecurityException(
        "Remote server is operating is untrusted mode, can not create remote actors on behalf of the remote client")

      val (interfaceClass, targetClass) =
        if (applicationLoader.isDefined) (applicationLoader.get.loadClass(interfaceClassname),
                                          applicationLoader.get.loadClass(targetClassname))
        else (Class.forName(interfaceClassname), Class.forName(targetClassname))

      val newInstance = TypedActor.newInstance(
        interfaceClass, targetClass.asInstanceOf[Class[_ <: TypedActor]], actorInfo.getTimeout).asInstanceOf[AnyRef]
      server.typedActors.put(parseUuid(uuid).toString, newInstance) // register by uuid
      newInstance
    } catch {
      case e: Throwable =>
        EventHandler.error(e, this, e.getMessage)
        server.notifyListeners(RemoteServerError(e, server))
        throw e
    }
  }

  private def createTypedActor(actorInfo: ActorInfoProtocol, channel: T): AnyRef = {
    val uuid = actorInfo.getUuid

    server.findTypedActorByIdOrUuid(actorInfo.getId, parseUuid(uuid).toString) match {
      case null => // the actor has not been registered globally. See if we have it in the session
        createTypedSessionActor(actorInfo, channel) match {
          case null => createClientManagedTypedActor(actorInfo) //Maybe client managed actor?
          case sessionActor => sessionActor
        }
      case typedActor => typedActor
    }
  }

  private def createErrorReplyMessage(exception: Throwable, request: RemoteMessageProtocol, actorType: AkkaActorType): AkkaRemoteProtocol = {
    val actorInfo = request.getActorInfo
    val messageBuilder = RemoteActorSerialization.createRemoteMessageProtocolBuilder(
      None,
      Right(request.getUuid),
      actorInfo.getId,
      actorInfo.getTarget,
      actorInfo.getTimeout,
      Left(exception),
      true,
      None,
      None,
      actorType,
      None)
    if (request.hasSupervisorUuid) messageBuilder.setSupervisorUuid(request.getSupervisorUuid)
    RemoteEncoder.encode(messageBuilder.build)
  }

  protected def parseUuid(protocol: UuidProtocol): Uuid = uuidFrom(protocol.getHigh,protocol.getLow)

  /**
   * Registers the pipelines with this server.
   */
  protected[akka] def registerPipelines(pipelines: Pipelines) = {
    this.currentPipelines.set(Some(pipelines))
  }

  /**
   * Unregisters the pipelines with this server.
   */
  protected[akka] def unregisterPipelines() = {
    this.currentPipelines.set(None)
  }

}
