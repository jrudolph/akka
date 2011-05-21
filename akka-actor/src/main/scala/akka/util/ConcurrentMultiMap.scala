/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */
package akka.util

import java.util.concurrent._
import scala.collection.JavaConversions._
import java.{ util => ju }

class ConcurrentMultiMap[K <: Comparable[K], V <: Comparable[V]] {

  private val map = new ConcurrentSkipListMap[K, ConcurrentSkipListSet[V]]()

  def add(key: K, value: V): Unit = {
    val candidate = new ConcurrentSkipListSet[V]
    val set = map.putIfAbsent(key, candidate)
    if (set eq null)
      candidate.add(value)
    else
      set.add(value)
  }

  def remove(key: K, value: V): Unit = {
    val set = map.get(key)
    if (set ne null) {
      set.remove(value)
    }
  }

  def get(key: K): ju.Set[V] = {
    map.get(key) match {
      case null => new ju.HashSet()
      case set => set
    }
  }

  def upto(key: K, inclusive: Boolean = true): Iterator[ju.Set[V]] = {
    map.headMap(key, inclusive).values.iterator
  }

  def from(key: K, inclusive: Boolean = true): Iterator[ju.Set[V]] = {
    map.tailMap(key, inclusive).values.iterator
  }

  def between(lower: K, lowerInclusive: Boolean, upper: K, upperInclusive: Boolean): Iterator[ju.Set[V]] = {
    map.subMap(lower, lowerInclusive, upper, upperInclusive).values.iterator
  }

  def all: Iterator[ju.Set[V]] = map.values.iterator

}
