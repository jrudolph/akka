/*
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.http.scaladsl.coding

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.http.impl.util.StreamUtils
import akka.stream.FlowShape
import akka.stream.impl.io.compression.Compressor
import akka.stream.stage.GraphStage
import akka.util.ByteString
import headers._
import akka.stream.scaladsl.Flow

trait Encoder {
  def encoding: HttpEncoding

  def messageFilter: HttpMessage ⇒ Boolean

  def encode[T <: HttpMessage](message: T)(implicit mapper: DataMapper[T]): T#Self =
    if (messageFilter(message) && !message.headers.exists(Encoder.isContentEncodingHeader))
      encodeData(message).withHeaders(`Content-Encoding`(encoding) +: message.headers)
    else message.self

  def encodeData[T](t: T)(implicit mapper: DataMapper[T]): T =
    mapper.transformDataBytes(t, Flow[ByteString].via(newEncodeTransformer))

  def encode(input: ByteString): ByteString = newCompressor.compressAndFinish(input)

  def encoderFlow: Flow[ByteString, ByteString, NotUsed] = Flow[ByteString].via(newEncodeTransformer)

  def newCompressor: Compressor

  def newEncodeTransformer(): GraphStage[FlowShape[ByteString, ByteString]] = {
    val compressor = newCompressor

    def encodeChunk(bytes: ByteString): ByteString = compressor.compressAndFlush(bytes)
    def finish(): ByteString = compressor.finish()

    StreamUtils.byteStringTransformer(encodeChunk, finish)
  }
}

object Encoder {
  val DefaultFilter: HttpMessage ⇒ Boolean = isCompressible _
  private[coding] def isCompressible(msg: HttpMessage): Boolean =
    msg.entity.contentType.mediaType.isCompressible

  private[coding] val isContentEncodingHeader: HttpHeader ⇒ Boolean = _.isInstanceOf[`Content-Encoding`]
}

