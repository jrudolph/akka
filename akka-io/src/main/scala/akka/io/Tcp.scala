/**
 * Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.io

import java.net.InetSocketAddress
import java.nio.channels.{ ServerSocketChannel, SocketChannel }
import akka.actor.ActorRef
import akka.util.ByteString

object Tcp {

  /// COMMANDS
  sealed trait Command

  case class Connect(remoteAddress: InetSocketAddress,
                     localAddress: Option[InetSocketAddress] = None) extends Command
  case class Bind(handler: ActorRef, address: InetSocketAddress, backlog: Int = 100) extends Command
  case class Register(handler: ActorRef) extends Command

  // TODO: what about close reasons?
  sealed trait CloseCommand extends Command

  case object Close extends CloseCommand
  case object ConfirmedClose extends CloseCommand
  case object Abort extends CloseCommand

  trait Write extends Command {
    def data: ByteString
    def isEmpty: Boolean
    def ack: AnyRef
    def nack: AnyRef

    /** Returns a new write with `numBytes` removed from the front */
    def consume(numBytes: Int): Write
    def consumeNack: Write
  }
  object Write {
    def empty: Write = EmptyWrite

    def apply(_data: ByteString): Write = apply(_data, null)
    def apply(_data: ByteString, _ack: AnyRef): Write = new Write {
      def data: ByteString = _data
      def isEmpty: Boolean = _data.isEmpty

      def ack: AnyRef = _ack
      def nack: AnyRef = null

      /** Returns a new write with `numBytes` removed from the front */
      def consume(numBytes: Int): Write =
        if (numBytes == 0) this
        else if (numBytes == _data.length) empty
        else apply(_data.drop(numBytes), _ack)

      def consumeNack: Write =
        this
    }
    def apply(_data: ByteString, _ack: AnyRef, _nack: AnyRef): Write = new Write {
      def data: ByteString = _data
      def isEmpty: Boolean = _data.isEmpty
      def ack: AnyRef = _ack
      def nack: AnyRef = _nack

      /** Returns a new write with `numBytes` removed from the front */
      def consume(numBytes: Int): Write =
        if (numBytes == 0) this
        else if (numBytes == _data.length) empty
        else apply(_data.drop(numBytes), _ack, _nack)

      def consumeNack: Write =
        apply(_data, _ack)
    }
  }

  /** The EmptyWrite, we use this in TcpBaseConnection as a marker for the empty write queue */
  object EmptyWrite extends Write {
    def data: ByteString = ByteString.empty
    def isEmpty: Boolean = true

    def consume(numBytes: Int): Write =
      if (numBytes == 0) this
      else throw new IllegalStateException("Can't consumed bytes from an EmptyWrite")

    def ack: AnyRef = throw new UnsupportedOperationException("Shouldn't be called on EmptyWrite")
    def nack: AnyRef = throw new UnsupportedOperationException("Shouldn't be called on EmptyWrite")
    def consumeNack: Write = throw new UnsupportedOperationException("Shouldn't be called on EmptyWrite")
  }

  case object StopReading extends Command
  case object ResumeReading extends Command

  /// EVENTS
  sealed trait Event

  case class Received(data: ByteString) extends Event
  case class Connected(localAddress: InetSocketAddress, remoteAddress: InetSocketAddress) extends Event

  sealed trait ConnectionClosed extends Event
  case object Closed extends ConnectionClosed
  case object Aborted extends ConnectionClosed
  case object ConfirmedClosed extends ConnectionClosed

  case object PeerClosed extends ConnectionClosed
  case class ErrorClose(cause: Throwable) extends ConnectionClosed

  /// INTERNAL
  case class RegisterClientChannel(channel: SocketChannel)
  case class RegisterServerChannel(channel: ServerSocketChannel)
  case class CreateConnection(channel: SocketChannel)
  case object ChannelConnectable
  case object ChannelAcceptable
  case object ChannelReadable
  case object ChannelWritable
  case object AcceptInterest
  case object ReadInterest
  case object WriteInterest
}
