package akka.io

import collection.immutable

import java.nio.channels.SocketChannel

import akka.actor.{ ActorLogging, Actor, ActorRef }

import Tcp.SocketOption

/**
 * An actor handling the connection state machine for an incoming, already connected
 * SocketChannel.
 */
class TcpIncomingConnection(val selector: ActorRef,
                            val commander: ActorRef,
                            val channel: SocketChannel,
                            val options: immutable.Seq[SocketOption])
  extends Actor
  with ActorLogging
  with TcpBaseConnection {

  context.watch(commander)

  channel.configureBlocking(false)
  completeConnect()

  def receive = PartialFunction.empty
}
