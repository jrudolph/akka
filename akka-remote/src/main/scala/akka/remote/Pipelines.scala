/**
 * Copyright (C) 2009-2010 Scalable Solutions AB <http://scalablesolutions.se>
 */

package akka.remote

import akka.remote.protocol.RemoteProtocol.RemoteMessageProtocol.Builder
import akka.remoteinterface.Filters

/**
 * Incoming and outgoing pipelines (vectors of functions that get passed the RemoteMessageProtocol.Builder of a message)
 */
private[akka] trait Pipelines {
  self: Filters =>
  /**
   * The incoming pipeline.
   * This method is called from the remote or client module when a message builder has been received and is going to be processed
   * when this method returns Some builder instead of None
   */
  private [akka] def incoming(builder: Builder): Option[Builder] = guard withReadGuard {
    _in.get.flatMap { vector =>
      executeFilters(builder, vector.iterator)
    }
  }

  /**
   * The outgoing pipeline.
   * This method is called from the remote or client module when a message builder is going to be sent when this method
   * returns Some builder instead of None.
   */
  private [akka] def outgoing(builder: Builder): Option[Builder] = guard withReadGuard {
    _out.get.flatMap { vector =>
      executeFilters(builder, vector.iterator)
    }
  }

  /**
   * returns if the incoming pipeline is empty
   */
  private [akka] def isIncomingEmpty = guard withReadGuard {
    _in.get match {
      case Some(pipeline) => pipeline.isEmpty
      case None=> true
    }
  }

  /**
   * returns if the outgoing pipeline is empty
   */
  private [akka] def isOutgoingEmpty = guard withReadGuard {
    _out.get match {
      case Some(pipeline) => pipeline.isEmpty
      case None=> true
    }
  }

  /**
   * Executes the filters, stops at None returned by a filter
   */
  private def executeFilters(builder: Builder, iterator: Iterator[Filter]): Option[Builder] = {
    if (iterator.isEmpty) {
      Some(builder)
    } else {
      var result: Any = builder
      while (iterator.hasNext && result != None) {
        result = iterator.next.apply(builder)
      }
      result match {
        case builder: Builder => Some(builder)
        case _ => None
      }
    }
  }
}