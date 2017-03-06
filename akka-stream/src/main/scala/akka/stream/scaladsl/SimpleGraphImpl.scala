package akka.stream.scaladsl

import scala.collection.mutable
import akka.{ Done, NotUsed }
import akka.dispatch.ExecutionContexts
import akka.event.NoLogging
import akka.stream._
import akka.stream.impl.fusing.GraphInterpreter.Connection
import akka.stream.impl.fusing.{ GraphInterpreter, GraphStages, Map }
import akka.stream.stage._

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable.VectorBuilder
import scala.concurrent.Future

trait MaterializationContext {
  def attributes: Attributes

  def addGraphStageLogic(logic: GraphStageLogic, shape: Shape): Unit
  def addConnection[T](out: Outlet[T], in: Inlet[T]): Unit
}

abstract class SimpleGraph[+S <: Shape, +M] {
  type Shape = S @uncheckedVariance
  //def shape: S
  def materialize(ctx: MaterializationContext): (S, M)
}

case class StageGraph[S <: Shape, +M](stage: GraphStageWithMaterializedValue[S, M]) extends SimpleGraph[S, M] {
  def materialize(ctx: MaterializationContext): (S, M) = {
    Materializer.initializeShape(stage.shape)
    val (logic, m) = stage.createLogicAndMaterializedValue(ctx.attributes)

    val shapeCopy = stage.shape.deepCopy.asInstanceOf[S] // we need to copy the shape here to allow keeping track of ports of multiple usages of the same GraphStage
    ctx.addGraphStageLogic(logic, shapeCopy)

    (shapeCopy, m)
  }
}

case class FlowAndFlow[In, Out1, Out2, M1, M2, M3](flow1: SimpleGraph[FlowShape[In, Out1], M1], flow2: SimpleGraph[FlowShape[Out1, Out2], M2], combineMat: (M1, M2) ⇒ M3) extends SimpleGraph[FlowShape[In, Out2], M3] {
  def materialize(ctx: MaterializationContext): (Shape, M3) = {
    val (s1, m1) = flow1.materialize(ctx)
    val (s2, m2) = flow2.materialize(ctx)
    ctx.addConnection(s1.out, s2.in)
    (FlowShape(s1.in, s2.out), combineMat(m1, m2))
  }
}

case class SourceViaGraph[Out, Out2, M1, M2, M3](source: SimpleGraph[SourceShape[Out], M1], flow: SimpleGraph[FlowShape[Out, Out2], M2], combineMat: (M1, M2) ⇒ M3) extends SimpleGraph[SourceShape[Out2], M3] {
  def materialize(ctx: MaterializationContext): (Shape, M3) = {
    val (s1, m1) = source.materialize(ctx)
    val (s2, m2) = flow.materialize(ctx)
    ctx.addConnection(s1.out, s2.in)
    (SourceShape(s2.out), combineMat(m1, m2))
  }
}

case class SourceAndSink[T, M1, M2, M3](source: SimpleGraph[SourceShape[T], M1], sink: SimpleGraph[SinkShape[T], M2], combineMat: (M1, M2) ⇒ M3) extends SimpleGraph[ClosedShape, M3] {
  def materialize(ctx: MaterializationContext): (Shape, M3) = {
    val (srcShape, m1) = source.materialize(ctx)
    val (sinkShape, m2) = sink.materialize(ctx)
    ctx.addConnection(srcShape.out, sinkShape.in)
    (ClosedShape, combineMat(m1, m2))
  }
}

/**
 * Seals a given graph, i.e. it connects all open inlets and outlets in the shape to an asynchronous boundary that
 * is connected to newly created inlets and outlets that are themselves connected to asynchronous boundaries.
 */
abstract class SealAsync[+S <: Shape, +M](inner: SimpleGraph[S, M]) extends SimpleGraph[S, M] {
  //def materialize(ctx: MaterializationContext,connectRest: (S, MaterializationContext) ⇒ MaterializationContext): M = ???
}

object Materializer {
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

  def run[M](runnableGraph: SimpleGraph[ClosedShape, M]): (M, GraphInterpreter) = {
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
      c.inHandler = c.inOwner.handlers(c.inInletId).asInstanceOf[InHandler]
      c.outHandler = c.outOwner.handlers(c.outOutletId + c.outOwner.inCount).asInstanceOf[OutHandler]
    }

    interpreter.init(null)

    (mat, interpreter)
  }
}

object MaterializedGraphInterpreterTest extends App {
  val singleSource = new GraphStages.SingleSource("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx")
  val map = new Map[AnyRef, AnyRef]({ x ⇒ println(x); x })
  val ignoreSink = GraphStages.IgnoreSink

  val runnable =
    SourceAndSink(
      SourceViaGraph(StageGraph(singleSource), StageGraph(map), Keep.none),
      StageGraph(ignoreSink),
      Keep.right[NotUsed, Future[Done]]
    )

  val (result, interpreter) = Materializer.run(runnable)

  var remainingSteps = 10
  while (!interpreter.isCompleted && remainingSteps > 0) {
    interpreter.execute(1)
    remainingSteps -= 1
    println(s"isCompleted: ${interpreter.isCompleted} remaining: $remainingSteps")
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