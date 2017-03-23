package akka.stream.scaladsl

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

object StuckSource extends App {
  val cfg = ConfigFactory.parseString("akka.loglevel = INFO").withFallback(ConfigFactory.load())
  implicit val system = ActorSystem("blub", cfg)
  val noFusingMaterializer =
    ActorMaterializer(ActorMaterializerSettings(system).withAutoFusing(false))

  /*
  works:

  val res =
    Source.empty[Int]
      .via(Flow[Int].map(_ + 1).mapConcat(x ⇒ List(x)))
      .toMat(Sink.ignore)(Keep.right)
      .run()(noFusingMaterializer)*/

  // stuck
  /*val res =
    Source.empty[Int]
      .runWith(
        Flow[Int]
          .via(Flow[Int]
            .map(_ + 1)
            .statefulMapConcat { () ⇒
              Thread.sleep(500)
              println("mapConcat initialized")
              x ⇒ List(x)
            }
          )
          .toMat(Sink.ignore)(Keep.right))(noFusingMaterializer)*/

  // works
  val res =
    Source.empty[Int]
      .runWith(
        Flow[Int]
          //.log("test")
          .via(Flow[Int]
            .map(_ + 1)
            .statefulMapConcat { () ⇒
              //Thread.sleep(500)
              println("mapConcat initialized")
              x ⇒ List(x)
            }
          )
          .toMat(Sink.ignore)(Keep.right))(noFusingMaterializer)

  try {
    val r = Await.result(res, 3000.millis)
    println(s"result was $r")
  } catch {
    case e ⇒ e.printStackTrace()
  } finally system.terminate()
}
