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
    case StopReading   ⇒ selector ! StopReading
    case ResumeReading ⇒ selector ! ReadInterest
    case ChannelReadable ⇒

      // new data arrived on the network
      val buffer = DirectBufferPool.get()
      val read = channel.read(buffer)
      buffer.flip()

      if (read > 0) {
        handler ! Received(ByteString(buffer).take(read))

        // try reading more
        // FIXME: loop here directly? if yes, how often?
        context.self ! ChannelReadable
      } else // we drained the incoming network buffer, back to waiting
        selector ! ReadInterest

    case write: Write if currentlyWriting ⇒
      if (write.nack != null)
        handler ! write.nack

    // drop packet

    case write: Write ⇒
      tryWrite(handler, write)

    case ChannelWritable ⇒
      assert(currentlyWriting)

      tryWrite(handler, remainingWrite)
  }

  def currentlyWriting =
    remainingWrite != null

  def tryWrite(handler: ActorRef, write: Write): Unit = {
    // data should be written on the network
    val data = write.data

    val buffer = DirectBufferPool.get()
    data.copyToBuffer(buffer)
    buffer.flip()
    val wrote = channel.write(buffer)

    remainingWrite =
      if (wrote == 0) { // send buffer full, try later
        selector ! WriteInterest

        write
      } else if (wrote < data.length) { // couldn't write all, try the rest later
        val newWrite = write.withData(data.drop(wrote))
        selector ! WriteInterest

        newWrite
      } else { // wrote == data.length, wrote everything
        if (write.ack != null)
          handler ! write.ack

        null
      }
  }

  def completeConnect(): Unit = {
    handler ! Connected(
      channel.getLocalAddress.asInstanceOf[InetSocketAddress],
      channel.getRemoteAddress.asInstanceOf[InetSocketAddress])

    context.become(waitingForRegistration)
  }
  def waitingForRegistration: Receive = {
    case Register(handler) ⇒
      selector ! ReadInterest

      context.become(connected(handler))
  }
}
