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

  // FIXME: put into settings proper
  val ReadBufferSize = 4096

  def connected(handler: ActorRef, writeQueue: Queue[Write]): Receive = {
    case Tcp.ChannelReadable ⇒
      // new data arrived on the network
      val buffer = ByteBuffer.allocate(ReadBufferSize)
      val read = channel.read(buffer)
      buffer.flip()

      if (read > 0) {
        // FIXME: compact or not?
        handler ! Received(ByteString.apply(buffer).take(read).compact)

        // try reading more
        context.self ! ChannelReadable
      } else // we drained the network buffer
        selector ! ReadInterest

    case writeMsg: Tcp.Write ⇒
      if (writeQueue.nonEmpty) {
        if (writeMsg.nack != null)
          handler ! writeMsg.nack

        // FIXME: should the nack send here?
        context.become(connected(handler, writeQueue.enqueue(writeMsg.consumeNack)))
      } else {
        val remaining = doWrite(handler, writeMsg)

        if (remaining != null)
          context.become(connected(handler, writeQueue.enqueue(remaining)))
        //else: we just wrote the message
      }

    case Tcp.ChannelWritable ⇒
      assert(writeQueue.nonEmpty)

      val first = writeQueue.head
      val remaining = doWrite(handler, first)
      if (remaining != null) // not all could be written, update head of queue
        context.become(connected(handler, remaining +: writeQueue.tail))
      else
        // we maybe could write even more but give the actor the possibility to
        // read (or do something else) in between
        context.self ! Tcp.ChannelWritable
  }

  def doWrite(handler: ActorRef, writeMsg: Tcp.Write): Tcp.Write = {
    // data should be written on the network
    val data = writeMsg.data

    val wrote = channel.write(data.asByteBuffer)
    if (wrote < data.length) {
      // couldn't write all data we have to queue the rest
      selector ! WriteInterest

      if (writeMsg.nack != null)
        handler ! writeMsg.nack

      // nack is sent only once only once so it is dropped here
      // consciously
      val newWrite = Write(data.drop(wrote), writeMsg.ack)

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

      context.become(connected(handler, Queue.empty))
  }
}
