package akka.io

import java.net.InetSocketAddress
import java.io.IOException
import java.nio.channels.SocketChannel

import collection.immutable

import scala.concurrent.duration._
import akka.actor._
import akka.util.ByteString

import Tcp._

/**
 * The base for TcpIncomingConnection and TcpOutgoingConnection.
 */
trait TcpBaseConnection extends Actor with WithDirectBuffer { _: Actor with ActorLogging ⇒
  def channel: SocketChannel
  def selector: ActorRef

  /** a write queue of size 1 to contain one unfinished write command */
  var remainingWrite: Write = Write.EmptyWrite
  def currentlyWriting = remainingWrite != Write.EmptyWrite

  // STATES

  /** connection established, waiting for registration from user handler */
  def waitingForRegistration(commander: ActorRef): Receive = {
    case Register(handler) ⇒
      log.debug("{} registered as connection handler for this connection", handler)

      selector ! ReadInterest

      context.setReceiveTimeout(Duration.Undefined)
      context.watch(handler) // sign death pact

      context.become(connected(handler))

    case cmd: CloseCommand ⇒ handleClose(commander, closeResponse(cmd))

    case ReceiveTimeout    ⇒ context.stop(self)
  }

  /** normal connected state */
  def connected(handler: ActorRef): Receive = {
    case StopReading     ⇒ selector ! StopReading
    case ResumeReading   ⇒ selector ! ReadInterest
    case ChannelReadable ⇒ doRead(handler)

    case write: Write if currentlyWriting ⇒
      log.debug("Dropping write because queue is already full")

      handler ! CommandFailed(write)
    case write: Write      ⇒ doWrite(handler, write)
    case ChannelWritable   ⇒ doWrite(handler, remainingWrite)

    case cmd: CloseCommand ⇒ handleClose(handler, closeResponse(cmd))
  }

  /** connection is closing but a write has to be finished first */
  def closingWithPendingWrite(handler: ActorRef, closedEvent: ConnectionClosed): Receive = {
    case ChannelReadable ⇒ doRead(handler)

    case ChannelWritable ⇒
      doWrite(handler, remainingWrite)

      if (!currentlyWriting) // write is now finished
        handleClose(handler, closedEvent)

    case Abort ⇒ handleClose(handler, Aborted)
  }

  /** connection is closed on our side and we're waiting from confirmation from the other side */
  def closing(handler: ActorRef): Receive = {
    case ChannelReadable ⇒ doRead(handler)
    case Abort           ⇒ handleClose(handler, Aborted)
  }

  // AUXILIARIES and IMPLEMENTATION

  /** use in subclasses to start the common machinery above once a channel is connected */
  def completeConnect(commander: ActorRef, options: immutable.Seq[SocketOption]): Unit = {
    options.foreach(_.afterConnect(channel.socket))

    commander ! Connected(
      channel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress],
      channel.socket.getRemoteSocketAddress.asInstanceOf[InetSocketAddress])

    context.setReceiveTimeout(Tcp(context.system).Settings.RegisterTimeout)

    context.become(waitingForRegistration(commander))
  }

  def doRead(handler: ActorRef): Unit = {
    val buffer = getDirectBuffer()

    try {
      log.debug("Trying to read from channel")

      val read = channel.read(buffer)
      buffer.flip()

      if (read > 0) {
        log.debug("Read returned {} bytes", read)

        handler ! Received(ByteString(buffer).take(read))

        // try reading more
        // FIXME: loop here directly? if yes, how often?
        self ! ChannelReadable
      } else if (read == 0) {
        log.debug("Read returned nothing. Registering read interest with selector", read)

        selector ! ReadInterest
      } else if (read == -1) {
        log.debug("Read returned end-of-stream", read)
        doCloseConnection(handler, closeReason)
      } else throw new IllegalStateException("Unexpected value returned from read: " + read)
    } catch {
      case e: IOException ⇒
        handleError(handler, e)
    }
  }

  def doWrite(handler: ActorRef, write: Write): Unit = {
    val data = write.data

    val buffer = getDirectBuffer()
    data.copyToBuffer(buffer)
    buffer.flip()

    try {
      log.debug("Trying to write to channel")

      val writtenBytes = channel.write(buffer)

      log.debug("Wrote {} bytes", writtenBytes)

      remainingWrite = consume(write, writtenBytes)
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
    if (closedEvent == Aborted) { // close instantly
      log.debug("Got abort command. RSTing connection.")

      doCloseConnection(handler, closedEvent)
    } else if (currentlyWriting) { // finish writing first
      log.debug("Got close command but write is still pending.")

      context.become(closingWithPendingWrite(handler, closedEvent))
    } else if (closedEvent == ConfirmedClosed) { // shutdown output and wait for confirmation
      log.debug("Got ConfirmedClose command, shutting down our side.")

      channel.socket.shutdownOutput()

      context.become(closing(handler))
    } else { // close now
      log.debug("Got Close command, closing connection.")

      doCloseConnection(handler, closedEvent)
    }
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
    exception.setStackTrace(Array.empty)
    handler ! ErrorClose(exception)

    throw exception
  }

  def abort(): Unit = {
    channel.socket.setSoLinger(true, 0)
    channel.close()
  }

  override def postStop() {
    if (channel.isOpen)
      abort()
  }

  /** Returns a new write with `numBytes` removed from the front */
  def consume(write: Write, numBytes: Int): Write = write match {
    case Write.EmptyWrite if numBytes == 0 ⇒ write
    case _ ⇒
      numBytes match {
        case 0                           ⇒ write
        case x if x == write.data.length ⇒ Write.empty
        case _ ⇒
          require(numBytes > 0 && numBytes < write.data.length)
          write.copy(data = write.data.drop(numBytes))
      }
  }
}
