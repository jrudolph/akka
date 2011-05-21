/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */
package akka.actor

import org.scalatest.{ WordSpec, BeforeAndAfterAll }
import org.scalatest.matchers.MustMatchers
import akka.testkit.{ TestKit, TestActorRef }
import akka.event.EventHandler
import Actor._
import akka.util.duration._

class ShutdownLevelSpec
    extends WordSpec
    with BeforeAndAfterAll
    with MustMatchers
    with TestKit {
  import ShutdownLevelSpec._

  override def beforeAll {
    Actor.registry.unregister(testActor)
  }

  override def afterAll {
    Actor.registry.register(testActor)
  }

  "An ActorRegistry" must {

    "shutdown actors in the right sequence" in {
      for (i <- 1 to 10) { actorOf(new LevelActor(i, testActor)).start() }
      Actor.registry.shutdownAll()
      val seq = receiveWhile(100 millis) {
        case Level(i) => i
      }
      seq must be (Seq(1,2,3,4,5,6,7,8,9,10))
    }

    "shutdown from a given level" in {
      for (i <- 1 to 10) { actorOf(new LevelActor(i, testActor)).start() }
      Actor.registry.shutdownAll(from = 5)
      val seq = receiveWhile(100 millis) {
        case Level(i) => i
      }
      seq must be (Seq(5,6,7,8,9,10))
      Actor.registry.shutdownAll()
      val seq2 = receiveWhile(100 millis) {
        case Level(i) => i
      }
      seq2 must be (Seq(1,2,3,4))
    }

    "shutdown up to a given level" in {
      for (i <- 1 to 10) { actorOf(new LevelActor(i, testActor)).start() }
      Actor.registry.shutdownAll(to = 5)
      val seq = receiveWhile(100 millis) {
        case Level(i) => i
      }
      seq must be (Seq(1,2,3,4,5))
      Actor.registry.shutdownAll()
      val seq2 = receiveWhile(100 millis) {
        case Level(i) => i
      }
      seq2 must be (Seq(6,7,8,9,10))
    }

    "shutdown given levels" in {
      for (i <- 1 to 10) { actorOf(new LevelActor(i, testActor)).start() }
      Actor.registry.shutdownAll(from = 4, to = 7)
      val seq = receiveWhile(100 millis) {
        case Level(i) => i
      }
      seq must be (Seq(4,5,6,7))
      Actor.registry.shutdownAll()
      val seq2 = receiveWhile(100 millis) {
        case Level(i) => i
      }
      seq2 must be (Seq(1,2,3,8,9,10))
    }

  }

}

object ShutdownLevelSpec {
  case class Level(i: Int)
  class LevelActor(override val shutdownLevel: Int, target: ActorRef) extends Actor {
    def receive = FSM.NullFunction
    override def postStop { target ! Level(shutdownLevel) }
  }
}
