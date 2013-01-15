package akka.io

import akka.actor.{ ReceiveTimeout, Terminated, ActorRef, Actor }
import scala.concurrent.duration._
import java.nio.channels.SocketChannel
import akka.io.Tcp._
import akka.util.ByteString
import java.net.{ StandardSocketOptions, InetSocketAddress }
import java.io.IOException

/**
 * The base for TcpIncomingConnection and TcpOutgoingCOnnection.
 */
trait TcpBaseConnection { _: Actor ⇒
  def channel: SocketChannel
  def selector: ActorRef
  def commander: ActorRef

  /** a write queue of size 1 to contain one unfinished write command */
  var remainingWrite: Write = EmptyWrite
  def currentlyWriting = !remainingWrite.isEmpty

  // STATES

  /** connection established, waiting for registration from user handler */
  def waitingForRegistration: Receive = {
    case Register(handler) ⇒
      selector ! ReadInterest

      context.setReceiveTimeout(Duration.Undefined)
      context.watch(handler)

      context.become(connected(handler))

    case cmd: CloseCommand                       ⇒ handleClose(commander, closeResponse(cmd))

    case ReceiveTimeout                          ⇒ context.stop(self)
    case Terminated(actor) if actor == commander ⇒ context.stop(self)
  }

  /** normal connected state */
  def connected(handler: ActorRef): Receive = {
    case StopReading                           ⇒ selector ! StopReading
    case ResumeReading                         ⇒ selector ! ReadInterest
    case ChannelReadable                       ⇒ doRead(handler)

    case write: Write if currentlyWriting      ⇒ handler ! CommandFailed(write)
    case write: Write                          ⇒ doWrite(handler, write)
    case ChannelWritable                       ⇒ doWrite(handler, remainingWrite)

    case cmd: CloseCommand                     ⇒ handleClose(handler, closeResponse(cmd))

    case Terminated(actor) if actor == handler ⇒ context.stop(self)
  }

  /** connection is closing but a write has to be finished first */
  def closingWithPendingWrite(handler: ActorRef, closedEvent: ConnectionClosed): Receive = {
    case ChannelReadable ⇒ doRead(handler)

    case ChannelWritable ⇒
      doWrite(handler, remainingWrite)

      if (!currentlyWriting) // write is now finished
        handleClose(handler, closedEvent)

    case Abort                                 ⇒ handleClose(handler, Aborted)

    case Terminated(actor) if actor == handler ⇒ context.stop(self)
  }

  /** connection is closed on our side and we're waiting from confirmation from the other side */
  def closing(handler: ActorRef): Receive = {
    case ChannelReadable                       ⇒ doRead(handler)
    case Abort                                 ⇒ handleClose(handler, Aborted)

    case Terminated(actor) if actor == handler ⇒ context.stop(self)
  }

  // AUXILIARIES and IMPLEMENTATION

  /** use in subclasses to start the common machinery above once a channel is connected */
  def completeConnect(): Unit = {
    commander ! Connected(
      channel.getLocalAddress.asInstanceOf[InetSocketAddress],
      channel.getRemoteAddress.asInstanceOf[InetSocketAddress])

    // FIXME: make configurable
    context.setReceiveTimeout(2.seconds)

    context.become(waitingForRegistration)
  }

  def doRead(handler: ActorRef): Unit = {
    val buffer = DirectBufferPool.get()

    try {
      val read = channel.read(buffer)
      buffer.flip()

      if (read > 0) {
        handler ! Received(ByteString(buffer).take(read))

        // try reading more
        // FIXME: loop here directly? if yes, how often?
        self ! ChannelReadable
      } else if (read == 0) selector ! ReadInterest
      else if (read == -1) doCloseConnection(handler, closeReason)
      else throw new IllegalStateException("Unexpected value returned from read: " + read)
    } catch {
      case e: IOException ⇒
        handleError(handler, e)
    }
  }

  def doWrite(handler: ActorRef, write: Write): Unit = {
    // data should be written on the network
    val data = write.data

    val buffer = DirectBufferPool.get()
    data.copyToBuffer(buffer)
    buffer.flip()

    try {
      val wrote = channel.write(buffer)

      remainingWrite = write.consume(wrote)
      if (!currentlyWriting && write.ack != null)
        handler ! write.ack

      // TODO: a possible optimization could be to try to send ourselves another
      // ChannelWritable now or soon after so we can avoid having to use the selector
      // in many cases. The downside is possibly spinning with low write rates.
      // if (currentlyWriting && wrote > 0)
      //   self ! ChannelWritable
      // else

      if (currentlyWriting) // still data to write
        selector ! WriteInterest
    } catch {
      case e: IOException ⇒
        handleError(handler, e)
    }
  }

  def closeReason =
    if (channel.socket().isOutputShutdown) ConfirmedClosed
    else PeerClosed

  def handleClose(handler: ActorRef, closedEvent: ConnectionClosed) {
    if (closedEvent == Aborted) // close instantly
      doCloseConnection(handler, closedEvent)
    else if (currentlyWriting) // finish writing first
      context.become(closingWithPendingWrite(handler, closedEvent))
    else if (closedEvent == ConfirmedClosed) { // shutdown output and wait for confirmation
      channel.socket.shutdownOutput()

      context.become(closing(handler))
    } else // close now
      doCloseConnection(handler, closedEvent)
  }

  def doCloseConnection(handler: ActorRef, closedEvent: ConnectionClosed): Unit = {
    if (closedEvent == Aborted) abort()
    else channel.close()

    handler ! closedEvent
    context.stop(self)
  }

  def closeResponse(closeCommand: CloseCommand): ConnectionClosed = closeCommand match {
    case Close          ⇒ Closed
    case Abort          ⇒ Aborted
    case ConfirmedClose ⇒ ConfirmedClosed
  }

  def handleError(handler: ActorRef, exception: IOException) {
    handler ! ErrorClose(exception)

    if (channel.isOpen)
      abort()

    context.stop(self)
  }

  def abort(): Unit = {
    channel.setOption(StandardSocketOptions.SO_LINGER, 0: Integer)
    channel.close()
  }

  override def postStop() {
    if (channel.isOpen)
      abort()
  }
}
