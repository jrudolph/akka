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
  case object Close extends Command
  case object ConfirmedClose extends Command
  case object Abort extends Command

  trait Write extends Command {
    def data: ByteString
    def ack: AnyRef
    def nack: AnyRef

    def withData(newData: ByteString): Write
    def consumeNack: Write
  }
  object Write {
    def apply(_data: ByteString): Write = apply(_data, null)
    def apply(_data: ByteString, _ack: AnyRef): Write = new Write {
      def data: ByteString = _data
      def ack: AnyRef = _ack
      def nack: AnyRef = null

      def withData(newData: ByteString): Write =
        apply(newData, _ack)

      def consumeNack: Write =
        this
    }
    def apply(_data: ByteString, _ack: AnyRef, _nack: AnyRef): Write = new Write {
      def data: ByteString = _data
      def ack: AnyRef = _ack
      def nack: AnyRef = _nack

      def withData(newData: ByteString): Write =
        apply(newData, _ack, _nack)

      def consumeNack: Write =
        apply(_data, _ack)
    }
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
