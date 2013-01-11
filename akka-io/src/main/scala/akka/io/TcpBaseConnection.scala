package akka.io

import akka.actor.{ ActorRef, Actor }
import java.nio.channels.SocketChannel
import akka.io.Tcp._
import java.nio.ByteBuffer
import akka.util.ByteString
import collection.immutable.Queue
import java.net.InetSocketAddress

trait TcpBaseConnection { self: Actor ⇒
  def channel: SocketChannel
  def selector: ActorRef
  def handler: ActorRef

  var remainingWrite: Write = null

  def connected(handler: ActorRef): Receive = {
    case Tcp.StopReading   ⇒ selector ! StopReading
    case Tcp.ResumeReading ⇒ selector ! ReadInterest
    case Tcp.ChannelReadable ⇒

      // new data arrived on the network
      val buffer = DirectBufferPool.get()
      val read = channel.read(buffer)
      buffer.flip()

      if (read > 0) {
        handler ! Received(ByteString.apply(buffer).take(read))

        // try reading more
        // FIXME: loop here directly? if yes, how often?
        context.self ! ChannelReadable
      } else // we drained the incoming network buffer
        selector ! ReadInterest

    case writeMsg: Tcp.Write ⇒
      if (remainingWrite != null) {
        if (writeMsg.nack != null)
          handler ! writeMsg.nack
      } else
        tryWrite(handler, writeMsg)

    case Tcp.ChannelWritable ⇒
      assert(remainingWrite != null)

      tryWrite(handler, remainingWrite)
  }

  def tryWrite(handler: ActorRef, writeMsg: Tcp.Write): Unit = {
    // data should be written on the network
    val data = writeMsg.data

    val buffer = DirectBufferPool.get()
    data.copyToBuffer(buffer)
    buffer.flip()
    val wrote = channel.write(buffer)

    remainingWrite =
      if (wrote == 0) {
        selector ! WriteInterest

        writeMsg
      } else if (wrote < data.length) {
        val newWrite = writeMsg.withData(data.drop(wrote))
        selector ! WriteInterest

        newWrite
      } else { // wrote == data.length, wrote everything
        if (writeMsg.ack != null)
          handler ! writeMsg.ack

        null
      }
  }

  def completeConnect: Unit = {
    handler ! Tcp.Connected(
      channel.getLocalAddress.asInstanceOf[InetSocketAddress],
      channel.getRemoteAddress.asInstanceOf[InetSocketAddress])

    context.become(waitingForRegistration)
  }
  def waitingForRegistration: Receive = {
    case Tcp.Register(handler) ⇒
      selector ! Tcp.ReadInterest

      context.become(connected(handler))
  }
}
