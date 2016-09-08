/*
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.http.scaladsl.coding

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.stream.impl.{ DeflateCompressor, DeflateDecompressor }

class Deflate(val messageFilter: HttpMessage ⇒ Boolean) extends Coder with StreamDecoder {
  val encoding = HttpEncodings.deflate
  def newCompressor = new DeflateCompressor
  def newDecompressorStage(maxBytesPerChunk: Int) = () ⇒ new DeflateDecompressor(maxBytesPerChunk)
}

object Deflate extends Deflate(Encoder.DefaultFilter)
