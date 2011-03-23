/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.actor.remote

import akka.actor.Actor._
import akka.testkit.TestKit
import akka.util.duration._
import akka.util.Address
import org.scalatest.matchers.ShouldMatchers
import java.lang.String
import akka.remote.netty.NettyRemoteSupport
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import akka.remote.protocol.RemoteProtocol.RemoteMessageProtocol.Builder
import akka.actor.Actor
import akka.remote.MessageSerializer
import com.google.protobuf.ByteString
import akka.remote.protocol.RemoteProtocol.MetadataEntryProtocol
import java.util.concurrent.{TimeUnit, CountDownLatch}
import collection.mutable.{ArrayBuffer, SynchronizedBuffer, ListBuffer}

/**
 * Test for Ticket #597, tests for pipelines
 */
class PipelineSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll with TestKit {
  val remote = Actor.remote
  val host = "127.0.0.1"
  val port = 25520
  val timeoutTestActor = 50
  val actorName = "test-name"
  val anotherName = "under another name"
  val echoName = "echo"

  def OptimizeLocal = false

  var optimizeLocal_? = remote.asInstanceOf[NettyRemoteSupport].optimizeLocalScoped_?

  override def beforeAll {
    setTestActorTimeout(timeoutTestActor seconds)
    remote.start(host, port)
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(false) //Can't run the test if we're eliminating all remote calls
    remote.register(actorName, testActor)
    remote.register(echoName, actorOf(new EchoActor))
  }

  override def afterAll {
    if (!OptimizeLocal)
      remote.asInstanceOf[NettyRemoteSupport].optimizeLocal.set(optimizeLocal_?) //Reset optimizelocal after all tests
    remote.shutdown
    Actor.registry.shutdownAll
    stopTestActor
  }


  "A registered client send filter" should {
    remote.clientFilters(host, port).clear
    remote.serverFilters.clear

    val pass = PassThrough(actorName)
    "get the request passed through it with a PassThrough filter" in {
      within(2 seconds) {
        remote.clientFilters(host, port).out(Vector(pass.filter))
        pass.interceptedMessages should have size (0)
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        pass.interceptedMessages should have size (1)
        pass.interceptedMessages map {
          builder => builder.getActorInfo.getId should be(actorName)
        }
      }
    }
    "be unregistered after clear" in {
      within(2 seconds) {
        remote.clientFilters(host, port).clear
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        pass.interceptedMessages should have size (1)
      }
    }
    "be functioning again after registering" in {
      within(2 seconds) {
        remote.clientFilters(host, port).out(Vector(pass.filter))
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        pass.interceptedMessages should have size (2)
      }
    }
  }
  "registered client filters pipeline" should {
    val pass1 = PassThrough(actorName)
    val pass2 = PassThrough(actorName)
    val passing = Vector[Any => Any](pass1.filter, pass2.filter)
    val filter = FilterByName(actorName)
    val filtering = Vector[Any => Any](pass1.filter, filter.filter, pass2.filter)

    "execute all filters" in {
      within(2 seconds) {
        remote.clientFilters(host, port).out(passing)
        pass1.interceptedMessages should have size (0)
        pass2.interceptedMessages should have size (0)
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
      }
      pass1.interceptedMessages should have size (1)
      pass2.interceptedMessages should have size (1)
    }
    "execute up until a filter stops passing messages" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear

      within(2 seconds) {
        remote.clientFilters(host, port).out(filtering)
        pass1.interceptedMessages should have size (1)
        filter.interceptedMessages should have size (0)
        pass2.interceptedMessages should have size (1)
        remote.actorFor(actorName, host, port) ! "test"
        expectNoMsg
      }
      pass1.interceptedMessages should have size (2)
      filter.interceptedMessages should have size (1)
      pass2.interceptedMessages should have size (1)
    }
  }

  "A registered client send filter" should {
    remote.clientFilters(host, port).clear
    remote.serverFilters.clear

    val filter = FilterByName(actorName)
    val remoteActor = remote.actorFor(actorName, host, port)
    "get the request passed through it and exclude the message with FilterByName" in {
      within(2 seconds) {
        remote.clientFilters(host, port).out(Vector(filter.filter))
        filter.interceptedMessages should have size (0)
        remoteActor ! "test"
        expectNoMsg
        filter.interceptedMessages should have size (1)
        filter.interceptedMessages map {
          protocol => protocol.getActorInfo.getId should be(actorName)
        }
      }
    }
    "not be active after another filter is set" in {
      within(2 seconds) {
        remote.clientFilters(host, port).out(Vector({
          a: Any => a
        }))
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        filter.interceptedMessages should have size (1)
      }
    }
    "be active after the filter is set again" in {
      within(2 seconds) {
        remote.clientFilters(host, port).out(Vector(filter.filter))
        remote.actorFor(actorName, host, port) ! "test"
        expectNoMsg
        filter.interceptedMessages should have size (2)
      }
    }
    "not be active after filters are cleared" in {
      within(2 seconds) {
        remote.clientFilters(host, port).clear
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        filter.interceptedMessages should have size (2)
      }
    }

    "get the request passed through it without FilterByName" in {
      within(2 seconds) {
        val anotherFilter = FilterByName(anotherName)
        remote.clientFilters(host, port).out(Vector(anotherFilter.filter))
        anotherFilter.interceptedMessages should have size (0)
        remote.actorFor(actorName, host, port) ! "test"
        expectMsg("test")
        anotherFilter.interceptedMessages should have size (0)
      }
    }

    "get the request passed through it and modify the message with Modify" in {
      val filter = Modify(echoName)
      remote.clientFilters(host, port).out(Vector(filter.filter))
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("changed the message in the pipeline")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }
    "get the request passed through it and add header to the message with AddMetaData" in {
      val addMetaData = AddMetaData(echoName, "key", "somedata")
      val getMetaData = GetMetaData(echoName)
      remote.clientFilters(host, port).out(Vector(addMetaData.filter))
      remote.serverFilters.in(Vector(getMetaData.filter))
      addMetaData.interceptedMessages should have size (0)
      getMetaData.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      addMetaData.interceptedMessages should have size (1)
      getMetaData.interceptedMessages should have size (1)
      addMetaData.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
      val header = getMetaData.header
      header._1 should be("key")
      header._2 should be("somedata")
    }
  }

  "A registered client receive filter" should {
    "get the reply passed through it" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear

      val filter = PassThrough(echoName)
      remote.clientFilters(host, port).in(Vector(filter.filter))
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }
    "timeout on reply when filtered by name in the outgoing server pipeline" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear

      val filter = FilterByName(echoName)
      remote.clientFilters(host, port).in(Vector(filter.filter))
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => fail("incorrect reply")
        case None=> //OK
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }
  }

  "A registered server receive filter" should {

    val filter = PassThrough(echoName)
    "get the request passed through it" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear
      remote.serverFilters.in(Vector(filter.filter))
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }

    "get no request passed through it after clear" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear
      filter.interceptedMessages should have size (1)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
    }
    "get request passed through it after registering again" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear

      remote.serverFilters.in(Vector(filter.filter))
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (2)
    }
    "timeout on reply when filtered by name in the incoming server pipeline" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear
      val filterIn = FilterByName(echoName)
      filterIn.interceptedMessages should have size (0)
      remote.serverFilters.in(Vector(filterIn.filter))
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => fail("incorrect reply")
        case None=> //OK
        case _ => fail("incorrect reply")
      }
      filterIn.interceptedMessages should have size (1)
    }
  }

  "A registered server send filter" should {
    "get the reply passed through it" in {
      val filter = PassThrough(echoName)
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear
      remote.serverFilters.out(Vector(filter.filter))
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("test")
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }
    "timeout on reply when filtered by name in the outgoing server pipeline" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear

      val filter = FilterByName(echoName)
      remote.serverFilters.out(Vector(filter.filter))
      filter.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => fail("incorrect reply")
        case None=> //OK
        case _ => fail("incorrect reply")
      }
      filter.interceptedMessages should have size (1)
      filter.interceptedMessages map {
        protocol => protocol.getActorInfo.getId should be(echoName)
      }
    }
  }

  "Registered vector of filters" should {
    "all be called when the filters return a builder" in {
      remote.serverFilters.clear
      val pass = PassThrough(echoName)
      val modify = Modify(echoName)
      val filters = Vector[Any => Any](pass.filter, modify.filter)
      remote.serverFilters.in(filters)
      pass.interceptedMessages should have size (0)
      modify.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("changed the message in the pipeline")
        case _ => fail("incorrect reply")
      }
      pass.interceptedMessages should have size (1)
      modify.interceptedMessages should have size (1)
    }
    "stop the chain of filters and message when a function returns None" in {
      val pass = PassThrough(echoName)
      val filter = FilterByName(echoName)
      // modify should not be called
      val modify = Modify(echoName)
      within(2 seconds) {
        remote.clientFilters(host, port).clear
        val filters = Vector[Any => Any](pass.filter, filter.filter, modify.filter)
        remote.clientFilters(host, port).out(filters)
        pass.interceptedMessages should have size (0)
        filter.interceptedMessages should have size (0)
        modify.interceptedMessages should have size (0)
        remote.actorFor(echoName, host, port) ! "test"
        expectNoMsg
      }
      pass.interceptedMessages should have size (1)
      filter.interceptedMessages should have size (1)
      modify.interceptedMessages should have size (0)
      //should timeout, return None
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => fail("incorrect reply")
        case None => //OK
        case _ => fail("incorrect reply")
      }
      pass.interceptedMessages should have size (2)
      filter.interceptedMessages should have size (2)
      modify.interceptedMessages should have size (0)
    }
  }

  "Modifying the server and client filters" should {
    "work when access to the filters is concurrent" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear

      // have two threads modify the clientfilters concurrently, one in, one out, client and server,  which should result in both in and out
      // correctly filtering messages at some point from both threads.
      val startLatch = new CountDownLatch(2)
      val latch = new CountDownLatch(2)
      val pass1 = PassThrough(echoName)
      val pass2 = PassThrough(echoName)
      val pass3 = PassThrough(echoName)
      val pass4 = PassThrough(echoName)
      val failures = new ArrayBuffer[Boolean]() with SynchronizedBuffer[Boolean]
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear
      // run a thread that adds incoming filters and !! messages
      val t1 = new Thread(new Runnable() {
        def run = {
          val localFailures = new ListBuffer[Boolean]()
          try {
            // let threads start (almost) simultaneously
            startLatch.countDown
            startLatch.await(1, TimeUnit.SECONDS)

            val begin = System.currentTimeMillis
            for (i <- 1 to 100) {
              // set the in a couple of times until a time is reached, the other thread should set out
              remote.clientFilters(host, port).in(Vector(pass1.filter))
              remote.serverFilters.in(Vector(pass3.filter))
              // send off some messages while doing this, should not interfere
              val reply = remote.actorFor(echoName, host, port) !! "thread1 while setting filters"
              reply match {
                case Some(msg) => if (!reply.get.equals("thread1 while setting filters")) localFailures.append(true)
                case _ => localFailures.append(true)
              }
            }
            for (i <- 1 to 100) {
              // send some messages
              val reply = remote.actorFor(echoName, host, port) !! "thread1"
              reply match {
                case Some(msg) => if (!reply.get.equals("thread1")) localFailures.append(true)
                case _ => localFailures.append(true)
              }
            }
            // loop while, check if the out filter is in effect, set from the other thread
            // send x messages
            // do the same for server filters
          } finally {
            latch.countDown
            failures.appendAll(localFailures)
          }
        }
      })
      // run a thread that adds outgoing filters and !! messages
      val t2 = new Thread(new Runnable() {
        def run = {
          val localFailures = new ListBuffer[Boolean]()
          try {
            // let threads start (almost) simultaneously
            startLatch.countDown
            startLatch.await(1, TimeUnit.SECONDS)
            val begin = System.currentTimeMillis
            var i = 0
            for (i <- 1 to 100) {
              // set the in a couple of times until a time is reached
              remote.clientFilters(host, port).out(Vector(pass2.filter))
              remote.serverFilters.out(Vector(pass4.filter))
              // send off some messages while doing this, should not interfere
              val reply = remote.actorFor(echoName, host, port) !! "thread2 while setting filters"
              reply match {
                case Some(msg) => if (!reply.get.equals("thread2 while setting filters")) localFailures.append(true)
                case _ => localFailures.append(true)
              }
            }
            for (i <- 1 to 100) {
              // send some messages
              val reply = remote.actorFor(echoName, host, port) !! "thread2"
              reply match {
                case Some(msg) => if (!reply.get.equals("thread2")) localFailures.append(true)
                case _ => localFailures.append(true)
              }
            }
            // check if the in filter is in effect, set from the other thread
            // send x messages
          } finally {
            latch.countDown
            failures.appendAll(localFailures)
          }
        }
      })
      pass1.interceptedMessages.size should be(0)
      pass2.interceptedMessages.size should be(0)
      pass3.interceptedMessages.size should be(0)
      pass4.interceptedMessages.size should be(0)
      t1.start
      t2.start
      // wait for threads to both finish
      latch.await(10, TimeUnit.SECONDS) should be(true)
      failures.size should be(0)

      // filters should have been called more often than the amounts of one thread, since both threads do !!, meaning filter has to also be triggered from the other thread
      pass1.interceptedMessages.size should be > (200)
      pass2.interceptedMessages.size should be > (200)
      pass3.interceptedMessages.size should be > (200)
      pass4.interceptedMessages.size should be > (200)

      // filters should have been called less or the same amount than the amounts of two threads, since both threads do !!, meaning filter has to also be triggered from the other thread
      pass1.interceptedMessages.size should be <= (400)
      pass2.interceptedMessages.size should be <= (400)
      pass3.interceptedMessages.size should be <= (400)
      pass4.interceptedMessages.size should be <= (400)
    }
  }

  "the server filters" should {
    "allow chaining of methods on the filters" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear

      val pass = PassThrough(echoName)
      val modify = Modify(echoName)
      val addMetaData = AddMetaData(echoName, "test-header", "val1")
      val getMetaData = GetMetaData(echoName)
      val incoming = Vector[Any => Any](addMetaData.filter, getMetaData.filter)
      val outgoing = Vector[Any => Any](pass.filter, modify.filter)
      remote.serverFilters.clear.in(incoming).out(outgoing)
      pass.interceptedMessages should have size (0)
      modify.interceptedMessages should have size (0)
      addMetaData.interceptedMessages should have size (0)
      getMetaData.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("changed the message in the pipeline")
        case _ => fail("incorrect reply")
      }
      pass.interceptedMessages should have size (1)
      modify.interceptedMessages should have size (1)
      addMetaData.interceptedMessages should have size (1)
      getMetaData.interceptedMessages should have size (1)
      getMetaData.header._1 should be("test-header")
      getMetaData.header._2 should be("val1")
    }
  }

  "the client filters" should {
    "allow chaining of methods on the filters" in {
      remote.clientFilters(host, port).clear
      remote.serverFilters.clear

      val pass = PassThrough(echoName)
      val modify = Modify(echoName)
      val addMetaData = AddMetaData(echoName, "test-header", "val1")
      val getMetaData = GetMetaData(echoName)
      val incoming = Vector[Any => Any](pass.filter, modify.filter)
      val outgoing = Vector[Any => Any](addMetaData.filter, getMetaData.filter)
      remote.clientFilters(host, port).clear.in(incoming).out(outgoing)
      pass.interceptedMessages should have size (0)
      modify.interceptedMessages should have size (0)
      addMetaData.interceptedMessages should have size (0)
      getMetaData.interceptedMessages should have size (0)
      val reply = remote.actorFor(echoName, host, port) !! "test"
      reply match {
        case Some(msg) => reply.get should be("changed the message in the pipeline")
        case _ => fail("incorrect reply")
      }
      pass.interceptedMessages should have size (1)
      modify.interceptedMessages should have size (1)
      addMetaData.interceptedMessages should have size (1)
      getMetaData.interceptedMessages should have size (1)
      getMetaData.header._1 should be("test-header")
      getMetaData.header._2 should be("val1")
    }
  }
}

case class FilterByName(id: String) {
  val messages = ListBuffer[Builder]()

  def interceptedMessages: ListBuffer[Builder] = messages

  def filter(any: Any): Any = {
    any match {
      case builder: Builder => {
        if (builder.getActorInfo.getId.equals(id)) {
          messages.append(builder.clone)
          None
        } else {
          builder
        }
      }
      case _ => None
    }
  }
}

case class AddMetaData(id: String, key: String, value: String) {
  val messages = ListBuffer[Builder]()

  def interceptedMessages: ListBuffer[Builder] = messages

  def filter(any: Any): Any = {
    any match {
      case builder: Builder => {
        if (builder.getActorInfo.getId.equals(id)) {
          messages.append(builder.clone)
          val bytes = ByteString.copyFromUtf8(value)
          val metadata = MetadataEntryProtocol.newBuilder.setKey(key).setValue(bytes)
          builder.addMetadata(metadata)
        } else {
          builder
        }
      }
      case _ => None
    }
  }
}

case class GetMetaData(id: String) {
  val messages = ListBuffer[Builder]()

  def interceptedMessages: ListBuffer[Builder] = messages

  def header: (String, String) = (messages.last.getMetadata(0).getKey, messages.last.getMetadata(0).getValue.toStringUtf8())

  def filter(any: Any): Any = {
    any match {
      case builder: Builder => {
        if (builder.getActorInfo.getId.equals(id)) {
          messages.append(builder.clone)
        }
        builder
      }
      case _ => None
    }
  }
}

case class Modify(id: String) {
  val messages = ListBuffer[Builder]()

  def interceptedMessages: ListBuffer[Builder] = messages

  def filter(any: Any): Any = {
    any match {
      case builder: Builder => {
        if (builder.getActorInfo.getId.equals(id)) {
          messages.append(builder.clone)
          val message = MessageSerializer.serialize(new String("changed the message in the pipeline"))
          builder.setMessage(message.toBuilder)
        } else {
          builder
        }
      }
      case _ => None
    }
  }
}

case class PassThrough(id: String) {
  val messages = ListBuffer[Builder]()

  def interceptedMessages: ListBuffer[Builder] = messages

  def filter(any: Any): Any = {
    any match {
      case builder: Builder => {
        messages.append(builder.clone())
        builder
      }
      case a: Any => {
        None
      }
    }
  }
}

class EchoActor extends Actor {
  def receive = {
    case msg => {
      self.reply(msg)
    }
  }
}
