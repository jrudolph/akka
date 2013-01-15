package akka.io

import akka.actor.{ Terminated, ActorRef, Actor }
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import collection.immutable
import akka.io.Tcp.{ SocketOption, RegisterClientChannel, ChannelConnectable }
import java.io.IOException

/**
 * An actor handling the connection state machine for an outgoing connection
 * to be established.
 */
class TcpOutgoingConnection(val selector: ActorRef,
                            val commander: ActorRef,
                            remoteAddress: InetSocketAddress,
                            localAddress: Option[InetSocketAddress],
                            val options: immutable.Seq[SocketOption]) extends Actor with TcpBaseConnection {
  val channel = openChannel()

  context.watch(commander)

  localAddress.foreach(channel.bind)
  options.foreach(_.beforeConnect(channel.socket))

  if (channel.connect(remoteAddress))
    completeConnect()
  else {
    selector ! RegisterClientChannel(channel)

    context.become(connecting)
  }

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
