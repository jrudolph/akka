package akka.io

import java.nio.ByteBuffer
import akka.actor.Actor

/**
 * Allows an actor to get a thread local direct buffer of a size defined in the
 * configuration of the actor system. An underlying assumption is that all of
 * the threads which call `getDirectBuffer` are owned by the actor system.
 */
trait WithDirectBuffer { _: Actor ⇒
  def getDirectBuffer(): ByteBuffer = {
    val result = WithDirectBuffer.threadLocalBuffer.get()
    if (result == null) {
      val size = Tcp(context.system).Settings.DirectBufferSize
      val newBuffer = ByteBuffer.allocateDirect(size)
      WithDirectBuffer.threadLocalBuffer.set(newBuffer)
      newBuffer
    } else result.clear().asInstanceOf[ByteBuffer]
  }
}

object WithDirectBuffer {
  private[WithDirectBuffer] val threadLocalBuffer = new ThreadLocal[ByteBuffer]
}
