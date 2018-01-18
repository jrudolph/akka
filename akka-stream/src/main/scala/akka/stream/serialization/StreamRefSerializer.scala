/**
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.serialization

import akka.actor.{ ActorRef, ExtendedActorSystem }
import akka.annotation.InternalApi
import akka.protobuf.ByteString
import akka.serialization.{ BaseSerializer, Serialization, SerializationExtension, SerializerWithStringManifest }
import akka.stream.scaladsl.SinkRefImpl
import akka.stream.scaladsl.SourceRefImpl
import akka.stream.{ StreamRefContainers, StreamRefs }
import akka.util.OptionVal

/** INTERNAL API */
@InternalApi
private[akka] final class StreamRefSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest
  with BaseSerializer {

  private[this] lazy val serialization = SerializationExtension(system)

  private[this] val OnSubscribeHandshakeManifest = "S"
  private[this] val SequencedOnNextManifest = "A"
  private[this] val CumulativeDemandManifest = "B"
  private[this] val RemoteSinkFailureManifest = "C"
  private[this] val RemoteSinkCompletedManifest = "D"
  private[this] val SourceRefManifest = "E"
  private[this] val SinkRefManifest = "F"

  override def manifest(o: AnyRef): String = o match {
    // protocol
    case _: StreamRefs.SequencedOnNext[_]    ⇒ SequencedOnNextManifest
    case _: StreamRefs.CumulativeDemand      ⇒ CumulativeDemandManifest
    // handshake
    case _: StreamRefs.OnSubscribeHandshake  ⇒ OnSubscribeHandshakeManifest
    // completion
    case _: StreamRefs.RemoteStreamFailure   ⇒ RemoteSinkFailureManifest
    case _: StreamRefs.RemoteStreamCompleted ⇒ RemoteSinkCompletedManifest
    // refs
    case _: SourceRefImpl[_]                 ⇒ SourceRefManifest
    case _: SinkRefImpl[_]                   ⇒ SinkRefManifest
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    // protocol
    case o: StreamRefs.SequencedOnNext[_]    ⇒ serializeSequencedOnNext(o).toByteArray
    case d: StreamRefs.CumulativeDemand      ⇒ serializeCumulativeDemand(d).toByteArray
    // handshake
    case h: StreamRefs.OnSubscribeHandshake  ⇒ serializeOnSubscribeHandshake(h).toByteArray
    // termination
    case d: StreamRefs.RemoteStreamFailure   ⇒ serializeRemoteSinkFailure(d).toByteArray
    case d: StreamRefs.RemoteStreamCompleted ⇒ serializeRemoteSinkCompleted(d).toByteArray
    // refs
    case ref: SinkRefImpl[_]                 ⇒ serializeSinkRef(ref).toByteArray
    case ref: SourceRefImpl[_]               ⇒ serializeSourceRef(ref).toByteArray
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    // protocol
    case OnSubscribeHandshakeManifest ⇒ deserializeOnSubscribeHandshake(bytes)
    case SequencedOnNextManifest      ⇒ deserializeSequencedOnNext(bytes)
    case CumulativeDemandManifest     ⇒ deserializeCumulativeDemand(bytes)
    case RemoteSinkCompletedManifest  ⇒ deserializeRemoteStreamCompleted(bytes)
    case RemoteSinkFailureManifest    ⇒ deserializeRemoteStreamFailure(bytes)
    // refs
    case SinkRefManifest              ⇒ deserializeSinkRef(bytes)
    case SourceRefManifest            ⇒ deserializeSourceRef(bytes)
  }

  // -----

  private def serializeCumulativeDemand(d: StreamRefs.CumulativeDemand): StreamRefContainers.CumulativeDemand = {
    StreamRefContainers.CumulativeDemand.newBuilder()
      .setSeqNr(d.seqNr)
      .build()
  }

  private def serializeRemoteSinkFailure(d: StreamRefs.RemoteStreamFailure): StreamRefContainers.RemoteStreamFailure = {
    StreamRefContainers.RemoteStreamFailure.newBuilder()
      .setCause(ByteString.copyFrom(d.msg.getBytes))
      .build()
  }

  private def serializeRemoteSinkCompleted(d: StreamRefs.RemoteStreamCompleted): StreamRefContainers.RemoteStreamCompleted = {
    StreamRefContainers.RemoteStreamCompleted.newBuilder()
      .setSeqNr(d.seqNr)
      .build()
  }

  private def serializeOnSubscribeHandshake(o: StreamRefs.OnSubscribeHandshake): StreamRefContainers.OnSubscribeHandshake = {
    StreamRefContainers.OnSubscribeHandshake.newBuilder()
      .setTargetRef(StreamRefContainers.ActorRef.newBuilder()
        .setPath(Serialization.serializedActorPath(o.targetRef)))
      .build()
  }

  private def serializeSequencedOnNext(o: StreamRefs.SequencedOnNext[_]) = {
    val p = o.payload.asInstanceOf[AnyRef]
    val msgSerializer = serialization.findSerializerFor(p)

    val payloadBuilder = StreamRefContainers.Payload.newBuilder()
      .setEnclosedMessage(ByteString.copyFrom(msgSerializer.toBinary(p)))
      .setSerializerId(msgSerializer.identifier)

    msgSerializer match {
      case ser2: SerializerWithStringManifest ⇒
        val manifest = ser2.manifest(p)
        if (manifest != "")
          payloadBuilder.setMessageManifest(ByteString.copyFromUtf8(manifest))
      case _ ⇒
        if (msgSerializer.includeManifest)
          payloadBuilder.setMessageManifest(ByteString.copyFromUtf8(p.getClass.getName))
    }

    StreamRefContainers.SequencedOnNext.newBuilder()
      .setSeqNr(o.seqNr)
      .setPayload(payloadBuilder.build())
      .build()
  }

  private def serializeSinkRef(sink: SinkRefImpl[_]): StreamRefContainers.SinkRef = {
    StreamRefContainers.SinkRef.newBuilder()
      .setTargetRef(StreamRefContainers.ActorRef.newBuilder()
        .setPath(Serialization.serializedActorPath(sink.initialPartnerRef)))
      .build()
  }

  private def serializeSourceRef(source: SourceRefImpl[_]): StreamRefContainers.SourceRef = {
    StreamRefContainers.SourceRef.newBuilder()
      .setOriginRef(
        StreamRefContainers.ActorRef.newBuilder()
          .setPath(Serialization.serializedActorPath(source.initialPartnerRef)))
      .build()
  }

  // ----------

  private def deserializeOnSubscribeHandshake(bytes: Array[Byte]): StreamRefs.OnSubscribeHandshake = {
    val handshake = StreamRefContainers.OnSubscribeHandshake.parseFrom(bytes)
    val targetRef = serialization.system.provider.resolveActorRef(handshake.getTargetRef.getPath)
    StreamRefs.OnSubscribeHandshake(targetRef)
  }

  private def deserializeSinkRef(bytes: Array[Byte]): SinkRefImpl[Any] = {
    val ref = StreamRefContainers.SinkRef.parseFrom(bytes)
    val initialTargetRef = serialization.system.provider.resolveActorRef(ref.getTargetRef.getPath)

    SinkRefImpl[Any](initialTargetRef)
  }

  private def deserializeSourceRef(bytes: Array[Byte]): SourceRefImpl[Any] = {
    val ref = StreamRefContainers.SourceRef.parseFrom(bytes)
    val initialPartnerRef = serialization.system.provider.resolveActorRef(ref.getOriginRef.getPath)

    SourceRefImpl[Any](initialPartnerRef)
  }

  private def deserializeSequencedOnNext(bytes: Array[Byte]): AnyRef = {
    val o = StreamRefContainers.SequencedOnNext.parseFrom(bytes)
    val p = o.getPayload
    val payload = serialization.deserialize(
      p.getEnclosedMessage.toByteArray,
      p.getSerializerId,
      p.getMessageManifest.toStringUtf8
    )
    StreamRefs.SequencedOnNext(o.getSeqNr, payload.get)
  }

  private def deserializeCumulativeDemand(bytes: Array[Byte]): StreamRefs.CumulativeDemand = {
    val d = StreamRefContainers.CumulativeDemand.parseFrom(bytes)
    StreamRefs.CumulativeDemand(d.getSeqNr)
  }
  private def deserializeRemoteStreamCompleted(bytes: Array[Byte]): StreamRefs.RemoteStreamCompleted = {
    val d = StreamRefContainers.RemoteStreamCompleted.parseFrom(bytes)
    StreamRefs.RemoteStreamCompleted(d.getSeqNr)
  }
  private def deserializeRemoteStreamFailure(bytes: Array[Byte]): AnyRef = {
    val d = StreamRefContainers.RemoteStreamFailure.parseFrom(bytes)
    StreamRefs.RemoteStreamFailure(d.getCause.toStringUtf8)
  }

}
