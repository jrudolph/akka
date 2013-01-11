package akka.io

import java.nio.ByteBuffer

// FIXME: should that be an IOExtension?
object DirectBufferPool {
  // make that configurable
  val defaultBufferSize = 16 * 1024

  val threadLocalBuffer = new ThreadLocal[ByteBuffer] {
    override def initialValue(): ByteBuffer =
      ByteBuffer.allocateDirect(defaultBufferSize)
  }

  def get(): ByteBuffer = {
    val res = threadLocalBuffer.get()
    res.clear()
    res
  }
}
