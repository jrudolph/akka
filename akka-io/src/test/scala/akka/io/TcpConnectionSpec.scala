package akka.io

import scala.annotation.tailrec

import akka.testkit.{ TestProbe, ImplicitSender, TestActorRef, AkkaSpec }
import java.net._
import java.nio.channels.{ SocketChannel, ServerSocketChannel }
import akka.io.Tcp._
import java.nio.ByteBuffer
import akka.util.ByteString
import scala.concurrent.duration._

class TcpConnectionSpec extends AkkaSpec with ImplicitSender {
  val port = 45679
  val localhost = InetAddress.getLocalHost
  val serverAddress = new InetSocketAddress(localhost, port)

  "An outgoing connection" must {
    // common behavior

    "go through the connection sequence" in withLocalServer() { localServer ⇒
      localServer.accept() must be(null)

      val userHandler = TestProbe()
      val connectionHandler = TestProbe()
      val selector = TestProbe()

      val conn = TestActorRef(
        new TcpOutgoingConnection(
          selector.ref,
          userHandler.ref,
          serverAddress,
          None))
      val clientChannel = conn.underlyingActor.channel

      // registered for interested
      selector.expectMsg(RegisterClientChannel(clientChannel))
      // still not connected
      clientChannel.isConnected must be(false)

      // server accepts
      val serverSideConnection = localServer.accept()
      serverSideConnection must not be (null)

      // still not connected because finishConnect will be called
      // only after selector tells it to
      clientChannel.isConnected must be(false)

      // flag connectable
      conn.tell(ChannelConnectable, selector.ref)

      // finished connection
      clientChannel.isConnected must be(true)

      // report connection establishment
      userHandler.expectMsg(Connected(clientChannel.getLocalAddress.asInstanceOf[InetSocketAddress], serverAddress))

      // register a connectionHandler for receiving data from now on
      conn.tell(Register(connectionHandler.ref), conn)

      selector.expectMsg(ReadInterest)
    }

    "send incoming data to user" in withEstablishedConnection() { setup ⇒
      import setup._

      serverSideChannel.write(ByteBuffer.wrap("testdata".getBytes("ASCII")))
      // emulate selector behavior
      connectionActor.tell(ChannelReadable, selector.ref)
      connectionHandler.expectMsgPF(remaining) {
        case Received(data) if data.decodeString("ASCII") == "testdata" ⇒
      }

      // have two packets in flight before the selector notices
      serverSideChannel.write(ByteBuffer.wrap("testdata2".getBytes("ASCII")))
      serverSideChannel.write(ByteBuffer.wrap("testdata3".getBytes("ASCII")))
      connectionActor.tell(ChannelReadable, selector.ref)
      connectionHandler.expectMsgPF(remaining) {
        case Received(data) if data.decodeString("ASCII") == "testdata2testdata3" ⇒
      }
    }
    "write data to network (and acknowledge)" in withEstablishedConnection() { setup ⇒
      import setup._

      serverSideChannel.configureBlocking(false)

      object Ack
      val write = Write(ByteString("testdata"), Ack)

      val buffer = ByteBuffer.allocate(100)
      serverSideChannel.read(buffer) must be(0)

      // emulate selector behavior
      connectionActor.tell(write, connectionHandler.ref)
      connectionHandler.expectMsg(Ack)

      serverSideChannel.read(buffer) must be(8)
      buffer.flip()
      ByteString(buffer).take(8).decodeString("ASCII") must be("testdata")
    }
    "stop writing in cases of backpressure and resume afterwards" in
      withEstablishedConnection(_.setOption(StandardSocketOptions.SO_RCVBUF, 1024: Integer)) { setup ⇒
        import setup._

        object Ack1
        object Ack2
        object NAck1
        object NAck2
        val TestSize = 10000

        def writeCmd(ack: AnyRef, nack: AnyRef) =
          Write(ByteString(Array.fill[Byte](TestSize)(0)), ack, nack)

        //serverSideChannel.configureBlocking(false)
        clientSideChannel.setOption(StandardSocketOptions.SO_SNDBUF, 1024: Integer)

        // producing backpressure by sending much more than currently fits into
        // our send buffer
        val firstWrite = writeCmd(Ack1, NAck1)

        // try to write the buffer but since the SO_SNDBUF is too small
        // it will have to keep the rest of the piece and send it
        // when possible
        connectionActor.tell(firstWrite, connectionHandler.ref)
        selector.expectMsg(WriteInterest)

        // send another write which should nack immediately
        // because we don't store more than one piece in flight
        connectionActor.tell(writeCmd(Ack2, NAck2), connectionHandler.ref)
        connectionHandler.expectMsg(NAck2)

        // there will be immediately more space in the SND_BUF because
        // some data will have been send now, so we assume we can write
        // again, but still it can't write everything
        connectionActor.tell(ChannelWritable, selector.ref)

        // both buffers should now be filled so no more writing
        // is possible

        // now drain on the other side until all has been transmitted
        val buffer = ByteBuffer.allocate(TestSize)
        @tailrec def drain(remaining: Int): Unit =
          if (remaining > 0) {
            if (selector.msgAvailable) {
              selector.expectMsg(WriteInterest)
              connectionActor.tell(ChannelWritable, selector.ref)
            }

            buffer.clear()
            val read = serverSideChannel.read(buffer)

            drain(remaining - read)
          }

        drain(TestSize)
        connectionHandler.expectMsg(Ack1)
      }

    "respect StopReading and ResumeReading" in withEstablishedConnection() { setup ⇒
      import setup._

      connectionActor.tell(StopReading, userHandler.ref)
      // the selector interprets StopReading to deregister interest
      // for reading
      selector.expectMsg(StopReading)

      connectionActor.tell(ResumeReading, userHandler.ref)
      selector.expectMsg(ReadInterest)
    }

    // error conditions
    "time out when user level Connected isn't answered with Register" in {

      pending
    }
    "report a connection error back to the user when the connection attempt fails" in {
      pending
    }
  }

  def withLocalServer(setServerSocketOptions: ServerSocketChannel ⇒ Unit = _ ⇒ ())(body: ServerSocketChannel ⇒ Any): Unit = {
    val localServer = ServerSocketChannel.open()
    try {
      setServerSocketOptions(localServer)
      localServer.bind(serverAddress)
      localServer.configureBlocking(false)
      body(localServer)
    } finally localServer.close()
  }

  case class Setup(
    userHandler: TestProbe,
    connectionHandler: TestProbe,
    selector: TestProbe,
    connectionActor: TestActorRef[TcpOutgoingConnection],
    clientSideChannel: SocketChannel,
    serverSideChannel: SocketChannel)
  def withEstablishedConnection(setServerSocketOptions: ServerSocketChannel ⇒ Unit = _ ⇒ ())(body: Setup ⇒ Any): Unit = withLocalServer(setServerSocketOptions) { localServer ⇒
    val userHandler = TestProbe()
    val connectionHandler = TestProbe()
    val selector = TestProbe()

    val connectionActor = TestActorRef(
      new TcpOutgoingConnection(
        selector.ref,
        userHandler.ref,
        serverAddress,
        None))

    val clientSideChannel = connectionActor.underlyingActor.channel

    selector.expectMsg(RegisterClientChannel(clientSideChannel))

    val serverSideChannel = localServer.accept()
    connectionActor.tell(ChannelConnectable, selector.ref)

    userHandler.expectMsg(Connected(clientSideChannel.getLocalAddress.asInstanceOf[InetSocketAddress], serverAddress))

    connectionActor.tell(Register(connectionHandler.ref), connectionActor)

    selector.expectMsg(ReadInterest)

    body {
      Setup(
        userHandler,
        connectionHandler,
        selector,
        connectionActor,
        clientSideChannel,
        serverSideChannel)
    }
  }
}
