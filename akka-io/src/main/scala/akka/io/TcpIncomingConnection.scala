package akka.io

import akka.actor.{ Actor, ActorRef }
import java.nio.channels.SocketChannel
import collection.immutable.Queue

class TcpIncomingConnection(val selector: ActorRef,
                            val handler: ActorRef,
                            val channel: SocketChannel) extends Actor with TcpBaseConnection {
  completeConnect()

  def receive = PartialFunction.empty
}
