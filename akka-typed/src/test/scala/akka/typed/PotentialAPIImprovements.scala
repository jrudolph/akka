package akka.typed

object PotentialAPIImprovements {
  trait Ctx[T] {
    def self: ActorRef[T]
    def deriveActorRef[U](f: U ⇒ T): ActorRef[U]
    def watch(actorRef: ActorRef[_]): Unit
  }

  trait Behavior[T]

  def Same[T]: Behavior[T] = ???

  def withContext[T](f: Ctx[T] ⇒ Behavior[T]): Behavior[T] = ???

  def handleMessages[T](f: T ⇒ Behavior[T]): Behavior[T] = handleMessagesAndSignals(f)(PartialFunction.empty)
  def handleMessagesAndSignals[T](f: T ⇒ Behavior[T])(signals: PartialFunction[Signal, Behavior[T]]): Behavior[T] = ???

  def recursively[T, U](initialState: U)(f: (U ⇒ Behavior[T]) ⇒ U ⇒ Behavior[T]): Behavior[T] = ???

  // current `Stateful` for comparison
  def functionalTwoArgs[T](f: (Ctx[T], T) ⇒ Behavior[T]): Behavior[T] = ???
}

object TestExample {
  sealed trait Command
  final case class GetSession(screenName: String, replyTo: ActorRef[SessionEvent])
    extends Command
  //#chatroom-protocol
  //#chatroom-behavior
  private final case class PostSessionMessage(screenName: String, message: String)
    extends Command
  //#chatroom-behavior
  //#chatroom-protocol

  sealed trait SessionEvent
  final case class SessionGranted(handle: ActorRef[PostMessage]) extends SessionEvent
  final case class SessionDenied(reason: String) extends SessionEvent
  final case class MessagePosted(screenName: String, message: String) extends SessionEvent

  final case class PostMessage(message: String)

  import PotentialAPIImprovements._

  def chatRoomMutable: Behavior[Command] =
    withContext[Command] { ctx ⇒
      var sessions: List[ActorRef[SessionEvent]] = Nil

      handleMessagesAndSignals[Command]({
        case GetSession(screenName, client) ⇒
          val wrapper = ctx.deriveActorRef {
            p: PostMessage ⇒ PostSessionMessage(screenName, p.message)
          }
          client ! SessionGranted(wrapper)
          sessions ::= client
          ctx.watch(client)
          Same
        case PostSessionMessage(screenName, message) ⇒
          val mp = MessagePosted(screenName, message)
          sessions foreach (_ ! mp)
          Same
      })({
        case Terminated(ref) ⇒
          sessions = sessions.filterNot(_ == ref)
          Same
      })
    }

  def chatRoomFunctional: Behavior[Command] = {
    def state(sessions: List[ActorRef[SessionEvent]]): Behavior[Command] =
      handleMessagesAndSignals[Command]({
        case GetSession(screenName, client) ⇒
          withContext { ctx ⇒
            val wrapper = ctx.deriveActorRef {
              p: PostMessage ⇒ PostSessionMessage(screenName, p.message)
            }
            client ! SessionGranted(wrapper)
            ctx.watch(client)
            state(client :: sessions)
          }
        case PostSessionMessage(screenName, message) ⇒
          val mp = MessagePosted(screenName, message)
          sessions foreach (_ ! mp)
          Same
      })({
        case Terminated(ref) ⇒ state(sessions.filterNot(_ == ref))
      })

    state(Nil)
  }

  def chatRoomFunctionalRecursive: Behavior[Command] =
    withContext { ctx ⇒
      recursively(List.empty[ActorRef[SessionEvent]]) { (state: List[ActorRef[SessionEvent]] ⇒ Behavior[Command]) ⇒ sessions: List[ActorRef[SessionEvent]] ⇒
        handleMessagesAndSignals[Command]({
          case GetSession(screenName, client) ⇒
            val wrapper = ctx.deriveActorRef {
              p: PostMessage ⇒ PostSessionMessage(screenName, p.message)
            }
            client ! SessionGranted(wrapper)
            ctx.watch(client)
            state(client :: sessions)
          case PostSessionMessage(screenName, message) ⇒
            val mp = MessagePosted(screenName, message)
            sessions foreach (_ ! mp)
            Same
        })({
          case Terminated(ref) ⇒ state(sessions.filterNot(_ == ref))
        })
      }
    }

  def chatRoomStatefulStyle: Behavior[Command] = {
    def state(sessions: List[ActorRef[SessionEvent]] = Nil): Behavior[Command] =
      functionalTwoArgs[Command] { (ctx, msg) ⇒
        msg match {
          case GetSession(screenName, client) ⇒
            val wrapper = ctx.deriveActorRef {
              p: PostMessage ⇒ PostSessionMessage(screenName, p.message)
            }
            client ! SessionGranted(wrapper)
            state(client :: sessions)
          case PostSessionMessage(screenName, message) ⇒
            val mp = MessagePosted(screenName, message)
            sessions foreach (_ ! mp)
            Same
        }
      }

    state()
  }

}