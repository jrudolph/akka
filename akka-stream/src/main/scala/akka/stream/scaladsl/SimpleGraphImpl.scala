package akka.stream.scaladsl

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }

import scala.collection.mutable
import akka.{ Done, NotUsed }
import akka.dispatch.ExecutionContexts
import akka.event.NoLogging
import akka.stream._
import akka.stream.impl.fusing.GraphInterpreter.Connection
import akka.stream.impl.fusing.{ GraphInterpreter, GraphStages, Map }
import akka.stream.scaladsl.Materializer.InterpreterCons
import akka.stream.stage.{ GraphStageLogic, _ }

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable.VectorBuilder
import scala.concurrent.Future

trait MaterializationContext {
  def attributes: Attributes

  def addGraphStageLogic(logic: GraphStageLogic, shape: Shape): Unit
  def addConnection[T](out: Outlet[T], in: Inlet[T]): Unit

  def actorOf(props: Props): ActorRef
  def system: ActorSystem
}

trait PreMaterializationContext {
  def attributes: Attributes

  def addGraphStage[S <: Shape, M](graph: GraphStageWithMaterializedValue[S, M], shape: S): MaterializationSession ⇒ M
  def addConnection[T](out: Outlet[T], in: Inlet[T]): Unit
}

trait MaterializationSession

abstract class SimpleGraph[+S <: Shape, +M] {
  type Shape = S @uncheckedVariance
  def materialize(ctx: MaterializationContext): (S, M)

  def preMaterialize(ctx: PreMaterializationContext): (S, MaterializationSession ⇒ M)
}

case class StageGraph[S <: Shape, +M](stage: GraphStageWithMaterializedValue[S, M]) extends SimpleGraph[S, M] {
  def preMaterialize(ctx: PreMaterializationContext): (S, MaterializationSession ⇒ M) = {
    Materializer.initializeShape(stage.shape)

    val shapeCopy = stage.shape.deepCopy.asInstanceOf[S] // we need to copy the shape here to allow keeping track of ports of multiple usages of the same GraphStage
    val acc = ctx.addGraphStage(stage, shapeCopy)

    (shapeCopy, acc)
  }

  def materialize(ctx: MaterializationContext): (S, M) = {
    Materializer.initializeShape(stage.shape)
    val (logic, m) = stage.createLogicAndMaterializedValue(ctx.attributes)

    val shapeCopy = stage.shape.deepCopy.asInstanceOf[S] // we need to copy the shape here to allow keeping track of ports of multiple usages of the same GraphStage
    ctx.addGraphStageLogic(logic, shapeCopy)

    (shapeCopy, m)
  }
}

case class FlowAndFlow[In, Out1, Out2, M1, M2, M3](flow1: SimpleGraph[FlowShape[In, Out1], M1], flow2: SimpleGraph[FlowShape[Out1, Out2], M2], combineMat: (M1, M2) ⇒ M3) extends SimpleGraph[FlowShape[In, Out2], M3] {

  def preMaterialize(ctx: PreMaterializationContext): (FlowShape[In, Out2], (MaterializationSession) ⇒ M3) = {
    val (s1, m1) = flow1.preMaterialize(ctx)
    val (s2, m2) = flow2.preMaterialize(ctx)
    ctx.addConnection(s1.out, s2.in)
    (FlowShape(s1.in, s2.out), session ⇒ combineMat(m1(session), m2(session)))
  }

  def materialize(ctx: MaterializationContext): (Shape, M3) = {
    val (s1, m1) = flow1.materialize(ctx)
    val (s2, m2) = flow2.materialize(ctx)
    ctx.addConnection(s1.out, s2.in)
    (FlowShape(s1.in, s2.out), combineMat(m1, m2))
  }
}

case class SourceViaGraph[Out, Out2, M1, M2, M3](source: SimpleGraph[SourceShape[Out], M1], flow: SimpleGraph[FlowShape[Out, Out2], M2], combineMat: (M1, M2) ⇒ M3) extends SimpleGraph[SourceShape[Out2], M3] {
  def preMaterialize(ctx: PreMaterializationContext): (SourceShape[Out2], (MaterializationSession) ⇒ M3) = {
    val (s1, m1) = source.preMaterialize(ctx)
    val (s2, m2) = flow.preMaterialize(ctx)
    ctx.addConnection(s1.out, s2.in)
    (SourceShape(s2.out), session ⇒ combineMat(m1(session), m2(session)))
  }

  def materialize(ctx: MaterializationContext): (Shape, M3) = {
    val (s1, m1) = source.materialize(ctx)
    val (s2, m2) = flow.materialize(ctx)
    ctx.addConnection(s1.out, s2.in)
    (SourceShape(s2.out), combineMat(m1, m2))
  }
}

case class FlowAndSink[In, Out, M1, M2, M3](flow: SimpleGraph[FlowShape[In, Out], M1], sink: SimpleGraph[SinkShape[Out], M2], combineMat: (M1, M2) ⇒ M3) extends SimpleGraph[SinkShape[In], M3] {
  def materialize(ctx: MaterializationContext): (SinkShape[In], M3) = {
    val (s1, m1) = flow.materialize(ctx)
    val (s2, m2) = sink.materialize(ctx)
    ctx.addConnection(s1.out, s2.in)
    (SinkShape(s1.in), combineMat(m1, m2))
  }

  def preMaterialize(ctx: PreMaterializationContext): (SinkShape[In], (MaterializationSession) ⇒ M3) = ???
}

case class SourceAndSink[T, M1, M2, M3](source: SimpleGraph[SourceShape[T], M1], sink: SimpleGraph[SinkShape[T], M2], combineMat: (M1, M2) ⇒ M3) extends SimpleGraph[ClosedShape, M3] {
  def preMaterialize(ctx: PreMaterializationContext): (ClosedShape, (MaterializationSession) ⇒ M3) = {
    val (srcShape, m1) = source.preMaterialize(ctx)
    val (sinkShape, m2) = sink.preMaterialize(ctx)
    ctx.addConnection(srcShape.out, sinkShape.in)
    (ClosedShape, session ⇒ combineMat(m1(session), m2(session)))
  }

  def materialize(ctx: MaterializationContext): (Shape, M3) = {
    val (srcShape, m1) = source.materialize(ctx)
    val (sinkShape, m2) = sink.materialize(ctx)
    ctx.addConnection(srcShape.out, sinkShape.in)
    (ClosedShape, combineMat(m1, m2))
  }
}

case class SealSink[In, M](inner: SimpleGraph[SinkShape[In], M]) extends SimpleGraph[SinkShape[In], M] {
  val sealedGraph = SourceAndSink(StageGraph(new InputBoundary[In]), inner, Keep.both[(SinkShape[In], GraphStageLogic), M])

  def materialize(ctx: MaterializationContext): (SinkShape[In], M) = {
    val (((shape, logic), m), interpreterCons) = Materializer.run(sealedGraph, ctx.system)

    ctx.actorOf(Props(new InterpreterActor(interpreterCons)))

    ctx.addGraphStageLogic(logic, shape)
    (shape, m)
  }

  def preMaterialize(ctx: PreMaterializationContext): (SinkShape[In], (MaterializationSession) ⇒ M) = ???

  class InputBoundary[T] extends GraphStageWithMaterializedValue[SourceShape[T], (SinkShape[T], GraphStageLogic)] {
    val out = Outlet[T]("InputBoundary.out")
    val in = Inlet[T]("InputBoundary.in")

    def shape: SourceShape[T] = SourceShape(out)

    def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, (SinkShape[T], GraphStageLogic)) = {
      val sinkShape = SinkShape(in)
      object SourceLogic extends GraphStageLogic(shape) with OutHandler {
        def pushAsync(element: T): Unit = runAsync(push(out, element))
        def completeAsync(): Unit = runAsync(completeStage())
        def failAsync(ex: Throwable): Unit = runAsync(failStage(ex))

        def runAsync(body: ⇒ Unit): Unit =
          // FIXME: we need to make sure this methods is only called when the logic was initialized
          getAsyncCallback[AnyRef](_ ⇒ body).invoke(null)

        sealed trait State
        case class WaitingForReady(ex: Seq[() ⇒ Unit]) extends State
        case object Ready extends State

        var state: State = WaitingForReady(Nil)
        def setReadyAsync(): Unit = runAsync {
          val fs = state.asInstanceOf[WaitingForReady].ex
          println(s"Now ready, running ${fs.size} methods")
          fs.foreach(_())
          state = Ready
        }
        def execute(body: ⇒ Unit): Unit =
          state match {
            case WaitingForReady(ex) ⇒ state = WaitingForReady(ex :+ (body _))
            case Ready               ⇒ body
          }

        setHandler(out, this)

        def onPull(): Unit = execute(SinkLogic.pullAsync())
        override def onDownstreamFinish(): Unit = execute(SinkLogic.cancelAsync())
      }
      object SinkLogic extends GraphStageLogic(sinkShape) with InHandler {
        def pullAsync(): Unit = runAsync {
          println("pulling")
          pull(in)
        }
        def cancelAsync(): Unit = runAsync {
          println("cancelling")
          cancel(in)
        }
        def runAsync(body: ⇒ Unit): Unit = getAsyncCallback[AnyRef](_ ⇒ body).invoke(null)

        override def preStart(): Unit = {
          setHandler(in, this)
          SourceLogic.setReadyAsync()
        }

        def onPush(): Unit = SourceLogic.pushAsync(grab(in))
        override def onUpstreamFinish(): Unit = SourceLogic.completeAsync()
        override def onUpstreamFailure(ex: Throwable): Unit = SourceLogic.failAsync(ex)
      }

      (SourceLogic, (SinkShape(in), SinkLogic))
    }
  }
}

/*case class SealFlow[In, Out, M](inner: SimpleGraph[FlowShape[In, Out] ,M]) extends SimpleGraph[FlowShape[In, Out] ,M] {
  def materialize(ctx: MaterializationContext): (FlowShape[In, Out], M) = {

  }

  def preMaterialize(ctx: PreMaterializationContext): (FlowShape[In, Out], (MaterializationSession) => M) = ???


}*/

/*
case class JoinAmorphous[M1, M2, M3](first: SimpleGraph[_, M1], second: SimpleGraph[_, M2], combineMat: (M1, M2) ⇒ M3) extends SimpleGraph[ClosedShape, M3] {
  def materialize(ctx: MaterializationContext): (ClosedShape, M3) = ???

  def preMaterialize(ctx: PreMaterializationContext): (ClosedShape, (MaterializationSession) ⇒ M3) = ???
}

/**
 * Seals a given graph, i.e. it connects all open inlets and outlets in the shape to an asynchronous boundary that
 * is connected to newly created inlets and outlets that are themselves connected to asynchronous boundaries.
 */
case class SealAsync[S <: Shape, M](inner: SimpleGraph[S, M]) extends SimpleGraph[S, M] {
  def materialize(ctx: MaterializationContext): (S, M) = {
    val (innerShape, innerMat) = inner.materialize(ctx)

    val async = new AsyncBoundaryStage(innerShape)
    val (logic, newShape) = async.createLogicAndMaterializedValue(ctx.attributes)
    ctx.addGraphStageLogic(logic, async.shape)
    (async.shape.outlets, innerShape.inlets).zipped.foreach((o, i) ⇒ ctx.addConnection(o.asInstanceOf[Outlet[AnyRef]], i.asInstanceOf[Inlet[AnyRef]]))
    (innerShape.outlets, async.shape.inlets).zipped.foreach((o, i) ⇒ ctx.addConnection(o.asInstanceOf[Outlet[AnyRef]], i.asInstanceOf[Inlet[AnyRef]]))

    (newShape, innerMat)
  }

  def recreateShapeShape(original: S, newShape: AmorphousShape): S = (original match {
    case SourceShape(out)   ⇒ SourceShape(newShape.outlets(0))
    case FlowShape(in, out) ⇒ FlowShape(newShape.inlets(0), newShape.outlets(0))
    case SinkShape(in)      ⇒ SinkShape(newShape.inlets(0))
  }).asInstanceOf[S]

  class AsyncBoundaryStage(counterpart: S) extends GraphStageWithMaterializedValue[AmorphousShape, Shape] {
    val shape: AmorphousShape =
      AmorphousShape(
        counterpart.outlets.map(o ⇒ Inlet[Any](o.s + "-counterpart")),
        counterpart.inlets.map(i ⇒ Outlet[Any](i.s + "-counterpart"))
      )

    def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, S) = {
      null
    }
  }

  def preMaterialize(ctx: PreMaterializationContext): (S, (MaterializationSession) ⇒ M) = ???
}
*/

object Materializer {
  type InterpreterCons = ((GraphStageLogic, Any, (Any) ⇒ Unit) ⇒ Unit, ActorRef) ⇒ GraphInterpreter

  def initializeShape(shape: Shape): Unit = {
    var i = 0
    while (i < shape.inlets.size) {
      shape.inlets(i).id = i
      i += 1
    }

    i = 0
    while (i < shape.outlets.size) {
      shape.outlets(i).id = i
      i += 1
    }
  }

  def run[M](runnableGraph: SimpleGraph[ClosedShape, M], _system: ActorSystem): (M, InterpreterCons) = {
    class MaterializationContextImpl extends MaterializationContext {
      def attributes: Attributes = Attributes.none

      var unconnectedInPorts = new mutable.HashMap[Inlet[_], (GraphStageLogic, Int)]()
      var unconnectedOutPorts = new mutable.HashMap[Outlet[_], (GraphStageLogic, Int)]()

      var nextLogicId = 0
      var logics = new VectorBuilder[GraphStageLogic]()
      var nextConnectionId = 0
      var connections = new VectorBuilder[Connection]()

      def addGraphStageLogic(logic: GraphStageLogic, shape: Shape): Unit = {
        val logicId = nextLogicId
        val logicEntry = logic → logicId

        shape.inlets.foreach(unconnectedInPorts.put(_, logicEntry))
        shape.outlets.foreach(unconnectedOutPorts.put(_, logicEntry))

        initializeShape(shape)

        logics += logic
        nextLogicId += 1
      }
      def addConnection[T](out: Outlet[T], in: Inlet[T]): Unit = {
        val Some((inOwner, inOwnerId)) = unconnectedInPorts.remove(in)
        val Some((outOwner, outOwnerId)) = unconnectedOutPorts.remove(out)

        val conn = new Connection(nextConnectionId, inOwnerId, inOwner, outOwnerId, outOwner, null, null, in.id, out.id)
        connections += conn

        inOwner.portToConn(in.id) = conn
        outOwner.portToConn(outOwner.inCount + out.id) = conn

        nextConnectionId += 1
      }

      def system: ActorSystem = _system
      def actorOf(props: Props): ActorRef = _system.actorOf(props)
    }

    val ctx = new MaterializationContextImpl
    val (_, mat) = runnableGraph.materialize(ctx)

    val logics = ctx.logics.result().toArray // FIXME: optimize
    var i = 0
    while (i < logics.size) {
      logics(i).stageId = i

      i += 1
    }

    val connections = ctx.connections.result().toArray // FIXME: optimize

    val interpreterCons: InterpreterCons = { (onAsyncInput, contextActorRef) ⇒
      val interpreter =
        new GraphInterpreter(
          NoMaterializer,
          NoLogging,
          logics,
          connections,
          onAsyncInput,
          fuzzingMode = false,
          contextActorRef
        )

      connections.foreach { c ⇒
        c.inHandler = c.inOwner.handlers(c.inInletId).asInstanceOf[InHandler]
        c.outHandler = c.outOwner.handlers(c.outOutletId + c.outOwner.inCount).asInstanceOf[OutHandler]
      }

      interpreter.init(null)
      interpreter
    }

    (mat, interpreterCons)
  }

  def preMaterialize[M](runnableGraph: SimpleGraph[ClosedShape, M]): () ⇒ (M, GraphInterpreter) = {
    class MaterializationSessionImpl(matVals: Array[Any]) extends MaterializationSession {
      def set(id: Int, value: Any): Unit = matVals(id) = value
      def get(id: Int): Any = matVals(id)
    }
    def getFromSession[M](id: Int): MaterializationSession ⇒ M =
      _.asInstanceOf[MaterializationSessionImpl].get(id).asInstanceOf[M]

    class MaterializationContextImpl extends PreMaterializationContext {
      def attributes: Attributes = Attributes.none

      type StageCons = GraphStageWithMaterializedValue[_ <: Shape, _]

      var unconnectedInPorts = new mutable.HashMap[Inlet[_], (StageCons, Int)]()
      var unconnectedOutPorts = new mutable.HashMap[Outlet[_], (StageCons, Int)]()

      var nextLogicId = 0
      var logics = new VectorBuilder[StageCons]()
      var nextConnectionId = 0
      var connections = new VectorBuilder[Connection]()

      def addGraphStage[S <: Shape, M](stage: GraphStageWithMaterializedValue[S, M], shape: S): MaterializationSession ⇒ M = {
        val logicId = nextLogicId
        val logicEntry = stage → logicId

        shape.inlets.foreach(unconnectedInPorts.put(_, logicEntry))
        shape.outlets.foreach(unconnectedOutPorts.put(_, logicEntry))

        initializeShape(shape)

        logics += stage
        nextLogicId += 1

        getFromSession(logicId)
      }
      def addConnection[T](out: Outlet[T], in: Inlet[T]): Unit = {
        val Some((inOwner, inOwnerId)) = unconnectedInPorts.remove(in)
        val Some((outOwner, outOwnerId)) = unconnectedOutPorts.remove(out)

        val conn = new Connection(nextConnectionId, inOwnerId, null, outOwnerId, null, null, null, in.id, out.id)
        connections += conn

        // FIXUPS in relation to strict materialization:
        // conn.inOwner
        // conn.outOwner
        // inOwner.portToConn(in.id) = conn
        // outOwner.portToConn(outOwner.inCount + out.id) = conn

        nextConnectionId += 1
      }
    }

    val ctx = new MaterializationContextImpl
    val (_, matCalculation) = runnableGraph.preMaterialize(ctx)

    val stages = ctx.logics.result().toArray // FIXME: optimize

    val materialize: () ⇒ (M, GraphInterpreter) = { () ⇒
      val session = new MaterializationSessionImpl(new Array(stages.size))

      val logics = new Array[GraphStageLogic](stages.size)
      var i = 0
      while (i < logics.size) {
        val (logic, mat) = stages(i).createLogicAndMaterializedValue(Attributes.none)
        logics(i) = logic
        session.set(i, mat)
        logics(i).stageId = i

        i += 1
      }

      val connections = ctx.connections.result().toArray // FIXME: need to really deep copy the array or the interpreters will share the instances

      val interpreter =
        new GraphInterpreter(
          NoMaterializer,
          NoLogging,
          logics,
          connections,
          (_, _, _) ⇒ throw new IllegalStateException("async input not allowed"),
          fuzzingMode = false,
          null
        )

      connections.foreach { c ⇒
        c.inOwner = logics(c.inOwnerId)
        c.outOwner = logics(c.outOwnerId)

        c.inOwner.portToConn(c.inInletId) = c
        c.outOwner.portToConn(c.outOutletId + c.outOwner.inCount) = c

        c.inHandler = c.inOwner.handlers(c.inInletId).asInstanceOf[InHandler]
        c.outHandler = c.outOwner.handlers(c.outOutletId + c.outOwner.inCount).asInstanceOf[OutHandler]
      }

      interpreter.init(null)

      (matCalculation(session), interpreter)
    }

    materialize
  }
}

object MaterializedGraphInterpreterTest extends App {
  val singleSource = new GraphStages.SingleSource("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx")
  val map = new Map[AnyRef, AnyRef]({ x ⇒ println(s"${Thread.currentThread().getName} $x"); x })
  val ignoreSink = GraphStages.IgnoreSink

  val mapFlow = StageGraph(map)

  val runnableTwoMaps =
    SourceAndSink(
      SourceViaGraph(
        SourceViaGraph(StageGraph(singleSource), mapFlow, Keep.none), mapFlow, Keep.none),
      StageGraph(ignoreSink),
      Keep.right[NotUsed, Future[Done]]
    )

  val runnablePrintlnOtherThread =
    SourceAndSink(
      StageGraph(singleSource),
      SealSink(
        FlowAndSink(
          mapFlow,
          StageGraph(ignoreSink),
          Keep.right[NotUsed, Future[Done]]
        )),
      Keep.right[NotUsed, Future[Done]]
    )

  val system = ActorSystem()

  try {
    //val (result, interpreter) = Materializer.preMaterialize(runnablePrintlnOtherThread)()
    val (result, interpreterCons) = Materializer.run(runnablePrintlnOtherThread, system)

    system.actorOf(Props(new InterpreterActor(interpreterCons)))
    result.onComplete { res ⇒
      println(res)
      system.terminate()
    }(system.dispatcher)

    /*val interpreter = interpreterCons((_, _, _) ⇒ throw new IllegalStateException("async input not allowed"), null)

    var remainingSteps = 10
    while (!interpreter.isCompleted && remainingSteps > 0) {
      interpreter.execute(1)
      remainingSteps -= 1
      println(s"isCompleted: ${interpreter.isCompleted} remaining: $remainingSteps")
    }*/
  } catch {
    case _ ⇒ system.terminate()
  }
}

object ManualGraphInterpreterTest extends App {
  // simple flow:
  //Source.single(1).map(x => {println(x); x}).runWith(Sink.ignore)

  val singleSource = new GraphStages.SingleSource("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx")
  val map = new Map[AnyRef, AnyRef]({ x ⇒ println(x); x })
  val ignoreSink = GraphStages.IgnoreSink

  singleSource.shape.out.id = 0
  ignoreSink.shape.in.id = 0
  map.shape.in.id = 0
  map.shape.out.id = 0 // in and out are counted differently

  val singleSourceLogic = singleSource.createLogic(Attributes.none)
  val mapLogic = map.createLogic(Attributes.none)
  val (ignoreSinkLogic, mat) = ignoreSink.createLogicAndMaterializedValue(Attributes.none)

  mat.onComplete(res ⇒ println(s"Got result: $res"))(ExecutionContexts.global)

  val singleMapConnection = new Connection(0, 1, mapLogic, 0, singleSourceLogic, null, null)
  val mapIgnoreConnection = new Connection(1, 2, ignoreSinkLogic, 1, mapLogic, null, null)

  singleSourceLogic.stageId = 0
  singleSourceLogic.portToConn(0) = singleMapConnection
  mapLogic.stageId = 1
  mapLogic.portToConn(0) = singleMapConnection
  mapLogic.portToConn(1) = mapIgnoreConnection
  ignoreSinkLogic.stageId = 2
  ignoreSinkLogic.portToConn(0) = mapIgnoreConnection

  val graphStageLogics: Array[GraphStageLogic] = Array(singleSourceLogic, mapLogic, ignoreSinkLogic)
  val connections: Array[Connection] = Array(singleMapConnection, mapIgnoreConnection)

  val interpreter =
    new GraphInterpreter(
      NoMaterializer,
      NoLogging,
      graphStageLogics,
      connections,
      (_, _, _) ⇒ throw new IllegalStateException("async input not allowed"),
      fuzzingMode = false,
      null
    )

  interpreter.init(null)
  singleMapConnection.outHandler = singleSourceLogic.handlers(0).asInstanceOf[OutHandler]
  singleMapConnection.inHandler = mapLogic.handlers(0).asInstanceOf[InHandler]

  mapIgnoreConnection.outHandler = mapLogic.handlers(1).asInstanceOf[OutHandler]
  mapIgnoreConnection.inHandler = ignoreSinkLogic.handlers(0).asInstanceOf[InHandler]

  println(s"isCompleted: ${interpreter.isCompleted}")

  var remainingSteps = 10
  while (!interpreter.isCompleted && remainingSteps > 0) {
    interpreter.execute(1)
    remainingSteps -= 1
    println(s"isCompleted: ${interpreter.isCompleted} remaining: $remainingSteps")
  }
}

case class Async(logic: GraphStageLogic, element: Any, handler: Any ⇒ Unit)
case object Resume
class InterpreterActor(cons: InterpreterCons) extends Actor {
  val interpreter = cons(defer, self)

  def defer(logic: GraphStageLogic, element: Any, handler: Any ⇒ Unit): Unit = self ! Async(logic, element, handler)

  override def preStart(): Unit = self ! Resume

  def receive = {
    case Async(logic, element, handler) ⇒
      interpreter.runAsyncInput(logic, element, handler)
      run()
    case Resume ⇒ run()
  }

  def run(): Unit = {
    interpreter.execute(50)
    if (interpreter.isCompleted) context.stop(self)
    else if (interpreter.isSuspended) self ! Resume
  }
}