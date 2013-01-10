package akka.io

import akka.actor.{ ActorRef, Actor }
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import collection.immutable.Queue

/**
 * Manages one client connection
 */
class TcpOutgoingConnection(val selector: ActorRef,
                            val handler: ActorRef,
                            remoteAddress: InetSocketAddress,
                            localAddress: Option[InetSocketAddress]) extends Actor with TcpBaseConnection {
  val channel = openChannel()

  override def preStart() {
    localAddress.foreach(channel.bind)

    val connected = channel.connect(remoteAddress)
    if (connected)
      completeConnect
    else {
      selector ! Tcp.RegisterClientChannel(channel)

      context.become(connecting)
    }
  }

  // fixme: do we do it like this?
  def receive: Receive = PartialFunction.empty

  def connecting: Receive = {
    case Tcp.ChannelConnectable ⇒
      val connected = channel.finishConnect()
      assert(connected, "Connectable channel failed to connect")
      completeConnect
  }

  def openChannel(): SocketChannel = {
    val channel = SocketChannel.open()
    channel.configureBlocking(false)
    channel
  }
}
