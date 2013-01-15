package akka.io

import scala.annotation.tailrec

import akka.testkit.{ TestProbe, ImplicitSender, TestActorRef, AkkaSpec }
import java.net._
import java.nio.channels.{ SelectionKey, Selector, SocketChannel, ServerSocketChannel }
import akka.io.Tcp._
import java.nio.ByteBuffer
import akka.util.ByteString
import scala.concurrent.duration._
import java.nio.channels.spi.SelectorProvider
import java.io.IOException
import akka.actor.{ Props, Actor, Terminated }

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
          None,
          Nil))
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
      selector.send(conn, ChannelConnectable)

      // finished connection
      clientChannel.isConnected must be(true)

      // report connection establishment
      userHandler.expectMsg(Connected(clientChannel.getLocalAddress.asInstanceOf[InetSocketAddress], serverAddress))

      // register a connectionHandler for receiving data from now on
      userHandler.send(conn, Register(connectionHandler.ref))

      selector.expectMsg(ReadInterest)
    }
    "set socket options before connecting" in withLocalServer() { localServer ⇒
      val userHandler = TestProbe()
      val selector = TestProbe()

      val connectionActor = TestActorRef(
        new TcpOutgoingConnection(
          selector.ref,
          userHandler.ref,
          serverAddress,
          None,
          Vector(SO.ReuseAddress(true))))

      val clientChannel = connectionActor.underlyingActor.channel
      clientChannel.getOption(StandardSocketOptions.SO_REUSEADDR).booleanValue() must be(true)
    }
    "set socket options after connecting" in withLocalServer() { localServer ⇒
      val userHandler = TestProbe()
      val selector = TestProbe()

      val connectionActor = TestActorRef(
        new TcpOutgoingConnection(
          selector.ref,
          userHandler.ref,
          serverAddress,
          None,
          Vector(SO.KeepAlive(true))))

      val clientChannel = connectionActor.underlyingActor.channel
      clientChannel.getOption(StandardSocketOptions.SO_KEEPALIVE).booleanValue() must be(false) // only set after connection is established
      selector.send(connectionActor, ChannelConnectable)
      clientChannel.getOption(StandardSocketOptions.SO_KEEPALIVE).booleanValue() must be(true)
    }

    "send incoming data to user" in withEstablishedConnection() { setup ⇒
      import setup._

      serverSideChannel.write(ByteBuffer.wrap("testdata".getBytes("ASCII")))
      // emulate selector behavior
      selector.send(connectionActor, ChannelReadable)
      connectionHandler.expectMsgPF(remaining) {
        case Received(data) if data.decodeString("ASCII") == "testdata" ⇒
      }

      // have two packets in flight before the selector notices
      serverSideChannel.write(ByteBuffer.wrap("testdata2".getBytes("ASCII")))
      serverSideChannel.write(ByteBuffer.wrap("testdata3".getBytes("ASCII")))
      selector.send(connectionActor, ChannelReadable)
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
      connectionHandler.send(connectionActor, write)
      connectionHandler.expectMsg(Ack)

      serverSideChannel.read(buffer) must be(8)
      buffer.flip()
      ByteString(buffer).take(8).decodeString("ASCII") must be("testdata")
    }
    "stop writing in cases of backpressure and resume afterwards" in
      withEstablishedConnection(setSmallRcvBuffer) { setup ⇒
        import setup._

        object Ack1
        object Ack2

        //serverSideChannel.configureBlocking(false)
        clientSideChannel.setOption(StandardSocketOptions.SO_SNDBUF, 1024: Integer)

        // producing backpressure by sending much more than currently fits into
        // our send buffer
        val firstWrite = writeCmd(Ack1)

        // try to write the buffer but since the SO_SNDBUF is too small
        // it will have to keep the rest of the piece and send it
        // when possible
        connectionHandler.send(connectionActor, firstWrite)
        selector.expectMsg(WriteInterest)

        // send another write which should fail the immediately
        // because we don't store more than one piece in flight
        val secondWrite = writeCmd(Ack2)
        connectionHandler.send(connectionActor, secondWrite)
        connectionHandler.expectMsg(CommandFailed(secondWrite))

        // there will be immediately more space in the SND_BUF because
        // some data will have been send now, so we assume we can write
        // again, but still it can't write everything
        selector.send(connectionActor, ChannelWritable)

        // both buffers should now be filled so no more writing
        // is possible
        setup.pullFromServerSide(TestSize)
        connectionHandler.expectMsg(Ack1)
      }

    "respect StopReading and ResumeReading" in withEstablishedConnection() { setup ⇒
      import setup._

      connectionHandler.send(connectionActor, StopReading)
      // the selector interprets StopReading to deregister interest
      // for reading
      selector.expectMsg(StopReading)

      connectionHandler.send(connectionActor, ResumeReading)
      selector.expectMsg(ReadInterest)
    }

    "close the connection" in withEstablishedConnection(setSmallRcvBuffer) { setup ⇒
      import setup._

      /*println("Port: " + clientSideChannel.getLocalAddress)
      val sel = SelectorProvider.provider().openSelector()
      serverSideChannel.configureBlocking(false)
      serverSideChannel.register(sel, SelectionKey.OP_READ)*/

      // we should test here that a pending write command is properly finished first
      object Ack
      // set an artificially small send buffer size so that the write is queued
      // inside the connection actor
      clientSideChannel.setOption(StandardSocketOptions.SO_SNDBUF, 1024: Integer)

      // we send a write and a close command directly afterwards
      connectionHandler.send(connectionActor, writeCmd(Ack))
      connectionHandler.send(connectionActor, Close)

      setup.pullFromServerSide(TestSize)
      connectionHandler.expectMsg(Ack)
      connectionHandler.expectMsg(Closed)
      connectionActor.isTerminated must be(true)

      //val res = sel.select()
      val buffer = ByteBuffer.allocate(1)
      serverSideChannel.read(buffer) must be(-1)
    }

    "abort the connection" in withEstablishedConnection() { setup ⇒
      import setup._

      connectionHandler.send(connectionActor, Abort)
      connectionHandler.expectMsg(Aborted)
      connectionActor.isTerminated must be(true)

      val buffer = ByteBuffer.allocate(1)
      val thrown = evaluating { serverSideChannel.read(buffer) } must produce[IOException]
      thrown.getMessage must be("Connection reset by peer")
    }

    "close the connection and confirm" in withEstablishedConnection(setSmallRcvBuffer) { setup ⇒
      import setup._

      // we should test here that a pending write command is properly finished first
      object Ack
      // set an artificially small send buffer size so that the write is queued
      // inside the connection actor
      clientSideChannel.setOption(StandardSocketOptions.SO_SNDBUF, 1024: Integer)

      // we send a write and a close command directly afterwards
      connectionHandler.send(connectionActor, writeCmd(Ack))
      connectionHandler.send(connectionActor, ConfirmedClose)

      setup.pullFromServerSide(TestSize)
      connectionHandler.expectMsg(Ack)

      selector.send(connectionActor, ChannelReadable)
      connectionHandler.expectNoMsg(100.millis) // not yet

      val buffer = ByteBuffer.allocate(1)
      serverSideChannel.read(buffer) must be(-1)
      serverSideChannel.close()

      selector.send(connectionActor, ChannelReadable)
      connectionHandler.expectMsg(ConfirmedClosed)
      connectionActor.isTerminated must be(true)
    }

    "peer closed the connection" in withEstablishedConnection() { setup ⇒
      import setup._

      serverSideChannel.close()

      selector.send(connectionActor, ChannelReadable)
      connectionHandler.expectMsg(PeerClosed)
      connectionActor.isTerminated must be(true)
    }
    "peer aborted the connection" in withEstablishedConnection() { setup ⇒
      import setup._

      serverSideChannel.setOption(StandardSocketOptions.SO_LINGER, 0: Integer)
      serverSideChannel.close()

      selector.send(connectionActor, ChannelReadable)
      connectionHandler.expectMsgPF(remaining) {
        case ErrorClose(exc: IOException) if exc.getMessage == "Connection reset by peer" ⇒
      }
      connectionActor.isTerminated must be(true)
    }
    "peer closed the connection when trying to write" in withEstablishedConnection() { setup ⇒
      import setup._

      serverSideChannel.setOption(StandardSocketOptions.SO_LINGER, 0: Integer)
      serverSideChannel.close()

      connectionHandler.send(connectionActor, Write(ByteString("testdata")))

      connectionHandler.expectMsgPF(remaining) {
        case ErrorClose(exc: IOException) ⇒ exc.getMessage must be("Connection reset by peer")
      }
      connectionActor.isTerminated must be(true)
    }

    // error conditions
    "report failed connection attempt while not registered" in withLocalServer() { localServer ⇒
      val userHandler = TestProbe()
      val selector = TestProbe()

      val connectionActor = TestActorRef(
        new TcpOutgoingConnection(
          selector.ref,
          userHandler.ref,
          serverAddress,
          None,
          Nil))

      val clientSideChannel = connectionActor.underlyingActor.channel
      selector.expectMsg(RegisterClientChannel(clientSideChannel))

      // close instead of accept
      localServer.close()

      selector.send(connectionActor, ChannelConnectable)
      userHandler.expectMsgPF() {
        case ErrorClose(e) ⇒ e.getMessage must be("Connection reset by peer")
      }
    }
    "report failed connection attempt when target is unreachable" in {
      val userHandler = TestProbe()
      val selector = TestProbe()

      val connectionActor = TestActorRef(
        new TcpOutgoingConnection(
          selector.ref,
          userHandler.ref,
          new InetSocketAddress("127.0.0.38", 4242), // some likely unknown address
          None,
          Nil))

      val clientSideChannel = connectionActor.underlyingActor.channel
      selector.expectMsg(RegisterClientChannel(clientSideChannel))

      val sel = SelectorProvider.provider().openSelector()
      val key = clientSideChannel.register(sel, SelectionKey.OP_CONNECT | SelectionKey.OP_READ)
      sel.select()

      key.isConnectable must be(true)
      selector.send(connectionActor, ChannelConnectable)

      userHandler.expectMsgPF() {
        case ErrorClose(e) ⇒ e.getMessage must be("Connection refused")
      }
    }
    "time out when user level Connected isn't answered with Register" in withLocalServer() { localServer ⇒
      val userHandler = TestProbe()
      val selector = TestProbe()

      val connectionActor = TestActorRef(
        new TcpOutgoingConnection(
          selector.ref,
          userHandler.ref,
          serverAddress,
          None,
          Nil))

      val watcher = TestProbe()
      watcher.watch(connectionActor)

      val clientSideChannel = connectionActor.underlyingActor.channel
      selector.expectMsg(RegisterClientChannel(clientSideChannel))

      localServer.accept()
      selector.send(connectionActor, ChannelConnectable)

      userHandler.expectMsg(Connected(clientSideChannel.getLocalAddress.asInstanceOf[InetSocketAddress], serverAddress))

      watcher.expectMsgPF(3.seconds) {
        case Terminated(`connectionActor`) ⇒
      }
      clientSideChannel.isOpen must be(false)
    }
    "close the connection when user handler dies while connecting" in withLocalServer() { localServer ⇒
      val userHandler = system.actorOf(Props(new Actor {
        def receive = PartialFunction.empty
      }))
      val selector = TestProbe()

      val connectionActor = TestActorRef(
        new TcpOutgoingConnection(
          selector.ref,
          userHandler,
          serverAddress,
          None,
          Nil))

      val watcher = TestProbe()
      watcher.watch(connectionActor)

      val clientSideChannel = connectionActor.underlyingActor.channel
      selector.expectMsg(RegisterClientChannel(clientSideChannel))

      system.stop(userHandler)

      watcher.expectMsgPF(1.seconds) {
        case Terminated(`connectionActor`) ⇒
      }
      clientSideChannel.isOpen must be(false)
    }
    "close the connection when connection handler dies while connected" in withEstablishedConnection() { setup ⇒
      import setup._

      val watcher = TestProbe()
      watcher.watch(connectionActor)

      system.stop(connectionHandler.ref)

      watcher.expectMsgPF(1.seconds) {
        case Terminated(`connectionActor`) ⇒
      }
      clientSideChannel.isOpen must be(false)
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
    serverSideChannel: SocketChannel) {

    val buffer = ByteBuffer.allocate(TestSize)
    @tailrec final def pullFromServerSide(remaining: Int): Unit =
      if (remaining > 0) {
        if (selector.msgAvailable) {
          selector.expectMsg(WriteInterest)
          selector.send(connectionActor, ChannelWritable)
        }

        buffer.clear()
        val read = serverSideChannel.read(buffer)
        if (read == 0)
          throw new IllegalStateException("Didn't make any progress")
        else if (read == -1)
          throw new IllegalStateException("Connection was closed unexpectedly with remaining bytes " + remaining)

        pullFromServerSide(remaining - read)
      }
  }
  def withEstablishedConnection(setServerSocketOptions: ServerSocketChannel ⇒ Unit = _ ⇒ ())(body: Setup ⇒ Any): Unit = withLocalServer(setServerSocketOptions) { localServer ⇒
    val userHandler = TestProbe()
    val connectionHandler = TestProbe()
    val selector = TestProbe()

    val connectionActor = TestActorRef(
      new TcpOutgoingConnection(
        selector.ref,
        userHandler.ref,
        serverAddress,
        None,
        Nil))

    val clientSideChannel = connectionActor.underlyingActor.channel

    selector.expectMsg(RegisterClientChannel(clientSideChannel))

    val serverSideChannel = localServer.accept()
    selector.send(connectionActor, ChannelConnectable)

    userHandler.expectMsg(Connected(clientSideChannel.getLocalAddress.asInstanceOf[InetSocketAddress], serverAddress))

    userHandler.send(connectionActor, Register(connectionHandler.ref))

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

  val TestSize = 10000
  def writeCmd(ack: AnyRef) =
    Write(ByteString(Array.fill[Byte](TestSize)(0)), ack)

  def setSmallRcvBuffer(channel: ServerSocketChannel) =
    channel.setOption(StandardSocketOptions.SO_RCVBUF, 1024: Integer)
}
