/**
 * Copyright (C) 2009-2011 Scalable Solutions AB <http://scalablesolutions.se>
 */
package akka.util

import java.util.concurrent._
import scala.collection.JavaConversions._
import java.{ util => ju }

/**
 * This map is a specialized implementation of a ConcurrentSortedMultiMap,
 * which is used for example in the ActorRegistry for handling shutdownLevel:
 * value sets are not removed when the last entry is dissociated from a key.
 * This has the advantage of allowing to function with less locking than a
 * "full" implementation, see akka.actor.Index in that case.
 *
 * Since this implementation is based upon ConcurrentSkipListMap/Set keys as
 * well as values are required to implement the Comparable interface (otherwise
 * ClassCastException would be thrown at runtime).
 */
class LevelMap[K <: Comparable[K], V <: Comparable[V]] {

  private val map = new ConcurrentSkipListMap[K, ConcurrentSkipListSet[V]]()

  /**
   * Add value to this key's set.
   */
  def add(key: K, value: V): Unit = {
    val candidate = new ConcurrentSkipListSet[V]
    val set = map.putIfAbsent(key, candidate)
    if (set eq null)
      candidate.add(value)
    else
      set.add(value)
  }

  /**
   * Remove value from this key's set. The set is NOT removed if this value was
   * the last member.
   */
  def remove(key: K, value: V): Unit = {
    val set = map.get(key)
    if (set ne null) {
      set.remove(value)
    }
  }

  /**
   * Get all values associated to a given key.
   */
  def get(key: K): ju.Set[V] = {
    map.get(key) match {
      case null => new ju.HashSet()
      case set => set
    }
  }

  /**
   * Get iterator over all sets whose key is smaller (or equal, if
   * inclusive is true) than the given key, in their natural order.
   */
  def upto(key: K, inclusive: Boolean = true): Iterator[ju.Set[V]] = {
    map.headMap(key, inclusive).values.iterator
  }

  /**
   * Get iterator over all sets whose key is greater (or equal, if
   * inclusive is true) than the given key, in their natural order.
   */
  def from(key: K, inclusive: Boolean = true): Iterator[ju.Set[V]] = {
    map.tailMap(key, inclusive).values.iterator
  }

  /**
   * Get iterator over all sets whose key is greater (or equal, if
   * inclusive is true) than the lower key and smaller (or equal, if inclusive
   * is true) than the upper key, in their natural order.
   */
  def between(lower: K, lowerInclusive: Boolean, upper: K, upperInclusive: Boolean): Iterator[ju.Set[V]] = {
    map.subMap(lower, lowerInclusive, upper, upperInclusive).values.iterator
  }

  /**
   * Get iterator over all sets in their natural order.
   */
  def all: Iterator[ju.Set[V]] = map.values.iterator

}
