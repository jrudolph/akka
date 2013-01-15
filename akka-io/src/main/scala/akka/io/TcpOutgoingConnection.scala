package akka.io

import akka.actor.{ ActorLogging, Terminated, ActorRef, Actor }
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
                            commander: ActorRef,
                            remoteAddress: InetSocketAddress,
                            localAddress: Option[InetSocketAddress],
                            options: immutable.Seq[SocketOption])
  extends Actor
  with ActorLogging
  with TcpBaseConnection {
  val channel = openChannel()

  context.watch(commander) // sign death pact

  localAddress.foreach(channel.socket.bind)
  options.foreach(_.beforeConnect(channel.socket))

  log.debug("Attempting connection to {}", remoteAddress)
  if (channel.connect(remoteAddress))
    completeConnect(commander, options)
  else {
    selector ! RegisterClientChannel(channel)

    context.become(connecting(commander, options))
  }

  def receive: Receive = PartialFunction.empty

  def connecting(commander: ActorRef, options: immutable.Seq[SocketOption]): Receive = {
    case ChannelConnectable ⇒
      try {
        val connected = channel.finishConnect()
        log.debug("Connection established")
        assert(connected, "Connectable channel failed to connect")
        completeConnect(commander, options)
      } catch {
        case e: IOException ⇒ handleError(commander, e)
      }
  }

  def openChannel(): SocketChannel = {
    val channel = SocketChannel.open()
    channel.configureBlocking(false)
    channel
  }
}
