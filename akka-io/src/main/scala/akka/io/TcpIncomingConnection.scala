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
                            commander: ActorRef,
                            val channel: SocketChannel,
                            options: immutable.Seq[SocketOption])
  extends Actor
  with ActorLogging
  with TcpBaseConnection {

  context.watch(commander)

  channel.configureBlocking(false)
  completeConnect(commander, options)

  def receive = PartialFunction.empty
}
