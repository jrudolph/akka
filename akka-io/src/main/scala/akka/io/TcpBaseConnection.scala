package akka.io

import akka.actor.{ ActorRef, Actor }
import java.nio.channels.SocketChannel
import akka.io.Tcp._
import java.nio.ByteBuffer
import akka.util.ByteString
import collection.immutable.Queue
import java.net.{ StandardSocketOptions, SocketOptions, InetSocketAddress }
import java.io.IOException

trait TcpBaseConnection { self: Actor ⇒
  def channel: SocketChannel
  def selector: ActorRef
  def handler: ActorRef

  var remainingWrite: Write = null

  def connected(handler: ActorRef): Receive = {
    case StopReading   ⇒ selector ! StopReading
    case ResumeReading ⇒ selector ! ReadInterest
    case ChannelReadable ⇒
      doRead(handler)

    case write: Write if currentlyWriting ⇒
      if (write.nack != null)
        handler ! write.nack

    // drop packet

    case write: Write ⇒
      doWrite(handler, write)

    case ChannelWritable ⇒
      assert(currentlyWriting)

      doWrite(handler, remainingWrite)

    case Close ⇒
      doCloseConnection(handler, confirmed = false)

    case Abort ⇒
      channel.setOption(StandardSocketOptions.SO_LINGER, 0: Integer)
      channel.close()
      handler ! Abort
      context.stop(context.self)

    case ConfirmedClose ⇒
      doCloseConnection(handler, confirmed = true)
  }

  def closingWithPendingWrite(handler: ActorRef, confirmed: Boolean): Receive = {
    case ChannelReadable ⇒
      doRead(handler)

    case ChannelWritable ⇒
      assert(currentlyWriting)

      doWrite(handler, remainingWrite)

      if (remainingWrite == null) // write is now finished
        doCloseConnection(handler, confirmed = confirmed)
  }

  def closing(handler: ActorRef): Receive = {
    case ChannelReadable ⇒
      doRead(handler)
  }

  def doRead(handler: ActorRef) {
    // new data arrived on the network
    val buffer = DirectBufferPool.get()

    try {
      val read = channel.read(buffer)
      buffer.flip()

      if (read > 0) {
        handler ! Received(ByteString(buffer).take(read))

        // try reading more
        // FIXME: loop here directly? if yes, how often?
        context.self ! ChannelReadable
      } else if (read == 0) // we drained the incoming network buffer, back to waiting
        selector ! ReadInterest
      else if (read == -1) {
        handler ! closeReason
        context.stop(context.self)
      } else
        throw new IllegalStateException("Unexpected value returned from read: " + read)
    } catch {
      case e: IOException ⇒
        handleError(handler, e)
    }
  }

  def closeReason =
    if (channel.socket().isOutputShutdown)
      ConfirmedClosed
    else
      PeerClosed

  def currentlyWriting =
    remainingWrite != null

  def handleError(handler: ActorRef, exception: IOException) {
    handler ! ErrorClose(exception)

    // FIXME: should we abort here instead?
    channel.close()
    context.stop(context.self)
  }

  def doWrite(handler: ActorRef, write: Write): Unit = {
    // data should be written on the network
    val data = write.data

    val buffer = DirectBufferPool.get()
    data.copyToBuffer(buffer)
    buffer.flip()

    try {
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
    } catch {
      case e: IOException ⇒
        handleError(handler, e)
    }
  }

  def doCloseConnection(handler: ActorRef, confirmed: Boolean) {
    if (currentlyWriting)
      context.become(closingWithPendingWrite(handler, confirmed = confirmed))
    else if (confirmed) {
      channel.socket.shutdownOutput()

      context.become(closing(handler))
    } else {
      channel.close()
      handler ! Closed
      context.stop(context.self)
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
