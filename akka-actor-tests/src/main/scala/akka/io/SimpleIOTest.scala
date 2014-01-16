package akka.io

import akka.actor.{ ActorRef, Props, ActorSystem, Actor }
import akka.io.{ Tcp, IO }
import akka.util.ByteString
import java.net.InetSocketAddress

object Response {
  val responseWrite = Tcp.Write(ByteString {
    "HTTP/1.1 200 OK\r\nServer: spray-can/1.3-M2\r\nDate: Tue, 07 Jan 2014 13:47:20 GMT\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Length: 10\r\n\r\nFAST-PONG!"
  })

  def writeAndClose(connection: ActorRef): Unit = {
    connection ! Response.responseWrite
    connection ! Tcp.ConfirmedClose
  }
}

class SimpleIOConnection extends Actor {
  def receive = {
    case r: Tcp.Received         ⇒ Response.writeAndClose(sender)
    case _: Tcp.ConnectionClosed ⇒ context.stop(self)
  }
}
class SimpleIOListener extends Actor {
  val connectProps = Props[SimpleIOConnection]
  def receive = {
    case c: Tcp.Connected ⇒ sender ! Tcp.Register(context.actorOf(connectProps))
  }
}

object SimpleIOListener extends App {
  implicit val system = ActorSystem()

  // the handler actor replies to incoming HttpRequests
  val handler = system.actorOf(Props[SimpleIOListener], name = "handler")

  IO(Tcp) ! Tcp.Bind(handler, new InetSocketAddress("localhost", 8080))
}
