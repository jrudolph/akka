/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import akka.NotUsed
import akka.dispatch.ExecutionContexts

import scala.collection.immutable
import scala.concurrent.Future

// should be Ops-ified to allow both usage in Flow and Source
trait SourceWithContext[+Ctx, +Out, +Mat] {
  def map[Out2](f: Out ⇒ Out2): SourceWithContext[Ctx, Out2, Mat]
  def mapAsync[Out2](parallelism: Int)(f: Out ⇒ Future[Out2]): SourceWithContext[Ctx, Out2, Mat]

  def collect[Out2](f: PartialFunction[Out, Out2]): SourceWithContext[Ctx, Out2, Mat]
  def filter(pred: Out ⇒ Boolean): SourceWithContext[Ctx, Out, Mat]

  def grouped(n: Int): SourceWithContext[immutable.Seq[Ctx], immutable.Seq[Out], Mat]

  def mapContext[Ctx2](f: Ctx ⇒ Ctx2): SourceWithContext[Ctx2, Out, Mat]

  def provideContext: Source[(Out, Ctx), Mat]
}

object SourceWithContext {
  def apply[Out, Mat](underlying: Source[Out, Mat]): SourceWithContext[Out, Out, Mat] =
    new SourceWithContextImpl[Out, Out, Mat](underlying.map(e ⇒ (e, e)))

  implicit class ContextSeqOps[Ctx, Out, Mat](s: SourceWithContext[immutable.Seq[Ctx], Out, Mat]) {
    def firstContext: SourceWithContext[Ctx, Out, Mat] = s.mapContext(_.head)
    def lastContext: SourceWithContext[Ctx, Out, Mat] = s.mapContext(_.last)
  }

  private class SourceWithContextImpl[+Ctx, +Out, +Mat](underlying: Source[(Out, Ctx), Mat]) extends SourceWithContext[Ctx, Out, Mat] {
    override def map[Out2](f: Out ⇒ Out2): SourceWithContext[Ctx, Out2, Mat] =
      via(flow.map { case (e, ctx) ⇒ (f(e), ctx) })

    override def mapAsync[Out2](parallelism: Int)(f: Out ⇒ Future[Out2]): SourceWithContext[Ctx, Out2, Mat] =
      via(flow.mapAsync(parallelism) { case (e, ctx) ⇒ f(e).map(o ⇒ (o, ctx))(ExecutionContexts.sameThreadExecutionContext) })

    override def collect[Out2](f: PartialFunction[Out, Out2]): SourceWithContext[Ctx, Out2, Mat] =
      via(flow.collect {
        case (e, ctx) if f.isDefinedAt(e) ⇒ (f(e), ctx)
      })
    override def filter(pred: Out ⇒ Boolean): SourceWithContext[Ctx, Out, Mat] =
      collect { case e if pred(e) ⇒ e }

    override def grouped(n: Int): SourceWithContext[immutable.Seq[Ctx], immutable.Seq[Out], Mat] =
      via(flow.grouped(n).map(elsWithContext ⇒ elsWithContext.unzip))

    override def mapContext[Ctx2](f: Ctx ⇒ Ctx2): SourceWithContext[Ctx2, Out, Mat] =
      via(flow.map { case (e, ctx) ⇒ (e, f(ctx)) })

    override def provideContext: Source[(Out, Ctx), Mat] = underlying

    private[this] def via[Out2, Ctx2](f: Flow[(Out, Ctx), (Out2, Ctx2), Any]): SourceWithContext[Ctx2, Out2, Mat] =
      new SourceWithContextImpl[Ctx2, Out2, Mat](underlying.via(f))
    private[this] def flow: Flow[(Out, Ctx), (Out, Ctx), NotUsed] = Flow[(Out, Ctx)]
  }
}
