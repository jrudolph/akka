package akka.io

import java.nio.ByteBuffer
import akka.actor.Actor

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
