/*
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.http.scaladsl.coding

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.stream.impl.io.compression.{ GzipCompressor, GzipDecompressor }

class Gzip(val messageFilter: HttpMessage ⇒ Boolean) extends Coder with StreamDecoder {
  val encoding = HttpEncodings.gzip
  def newCompressor = new GzipCompressor
  def newDecompressorStage(maxBytesPerChunk: Int) = () ⇒ new GzipDecompressor(maxBytesPerChunk)
}

/**
 * An encoder and decoder for the HTTP 'gzip' encoding.
 */
object Gzip extends Gzip(Encoder.DefaultFilter) {
  def apply(messageFilter: HttpMessage ⇒ Boolean) = new Gzip(messageFilter)
}
