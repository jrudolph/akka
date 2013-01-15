package akka.io

import akka.actor.{ Terminated, ActorRef, Actor }
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import collection.immutable.Queue
import akka.io.Tcp.{ RegisterClientChannel, ChannelConnectable }
import java.io.IOException

/**
 * An actor handling the connection state machine for an outgoing connection
 * to be established.
 */
class TcpOutgoingConnection(val selector: ActorRef,
                            val commander: ActorRef,
                            remoteAddress: InetSocketAddress,
                            localAddress: Option[InetSocketAddress]) extends Actor with TcpBaseConnection {
  val channel = openChannel()

  context.watch(commander)

  localAddress.foreach(channel.bind)

  val connected = channel.connect(remoteAddress)
  if (connected)
    completeConnect()
  else {
    selector ! RegisterClientChannel(channel)

    context.become(connecting)
  }

  // fixme: do we do it like this?
  def receive: Receive = PartialFunction.empty

  def connecting: Receive = {
    case ChannelConnectable ⇒
      try {
        val connected = channel.finishConnect()
        assert(connected, "Connectable channel failed to connect")
        completeConnect()
      } catch {
        case e: IOException ⇒ handleError(commander, e)
      }

    case Terminated(`commander`) ⇒ context.stop(self)
  }

  def openChannel(): SocketChannel = {
    val channel = SocketChannel.open()
    channel.configureBlocking(false)
    channel
  }
}
