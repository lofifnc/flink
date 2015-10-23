/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.test.tool.matcher

import org.scalatest.Matchers

import scala.collection.immutable.HashMap


/**
 * Wrapper around the [[Matchers]] library from ScalaTest.
 * @see http://scalatest.org/
 * Offers several methods to create different [[ListMatcher]]s working on lists.
 */
object ListMatchers extends Matchers {


  /**
   * Provides a [[ListMatcher]] to tests whether a list contains only a set of elements.
   * @example List(1,2,3,4) matched against List(1,2,3) is not valid.
   * @param right expected list of elements
   * @tparam T type to match
   * @return concrete [[ListMatcher]]
   */
  def containsOnly[T](right: List[T]): ListMatcher[T] = {
    new ListMatcher[T](right) {
      override def matches(left: List[T]) =
        left should contain only (right: _*)

      override def toString : String = {
        "only matcher"
      }
    }
  }

  /**
   * Provides a [[ListMatcher]] to tests whether a list contains a set of elements.
   * @example List(1,3,2,4) matched against List(1,2,3) is valid.
   * @param right expected list of elements.
   * @tparam T type to match
   * @return concrete [[ListMatcher]]
   */
  def containsAll[T](right: List[T]): ListMatcher[T] = {
    new ListMatcher[T](right) {
      override def matches(left: List[T]) = {
        val (first, second, rest) = splitTo(right)
        left should contain allOf(first, second, rest: _*)
      }

      override def toString : String = {
        "all matcher"
      }
    }
  }

  /**
   * Provides a [[ListMatcher]] to tests whether a list contains a sequence of elements.
   * The matcher permits other elements between the ordered elements.
   * also allows for duplicates.
   * @example List(1,2,4,3,3,5) matched against List(1,2,3) is valid.
   * @param right expected order of elements
   * @tparam T type to match
   * @return concrete [[ListMatcher]]
   */
  def containsInOrder[T](right: List[T]): ListMatcher[T] = {
    new ListMatcher[T](right) {
      override def matches(left: List[T]) = {
        val (first, second, rest) = splitTo(right)
        left should contain inOrder(first, second, rest: _*)
      }

      override def toString : String = {
        "order matcher"
      }
    }
  }


  /**
   * Provides a [[ListMatcher]] to tests whether a list contains another list
   * @example List(1,2,3,4) matched against List(2,3) is valid.
   * @param right expected list
   * @tparam T type to match
   * @return concrete [[ListMatcher]]
   */
  def containsInSeries[T](right: List[T]): ListMatcher[T] = {
    new ListMatcher[T](right) {
      override def matches(left: List[T]) = {

        if(!left.containsSlice(right)) {
          fail(s"output did not contain sequence $right")
        }
      }

      override def toString : String = {
        "series matcher"
      }
    }
  }

  /**
   * Provides a [[ListMatcher]] to tests whether a list contains
   * an element more often than another list.
   *
   * @example List(1,2,2,3,4,4) matched against List(1,2,2) is valid.
   * @param right expected list
   * @tparam T type to match
   * @return concrete [[ListMatcher]]
   */
  def containsNoDuplicates[T](right: List[T]): ListMatcher[T] = {
    new ListMatcher[T](right) {
      override def matches(left: List[T]) = {

        val countDuplicates = (l: List[T]) => l
          .groupBy(identity)
          .mapValues(_.size)

        val leftDuplicates = countDuplicates(left)
        val rightDuplicates = countDuplicates(right)

        rightDuplicates.foreach{
          case (elem, count) =>
            if(leftDuplicates(elem) > count) {
              fail(s" $elem should only be $count times in output")
            }
        }

      }

      override def toString : String = {
        "duplicate matcher"
      }
    }
  }

  /**
   * Helper function to split a list into a [[Tuple3]].
   * @param list to split
   * @return (first element, second element, rest of elements)
   */
  private def splitTo(list: List[Any]): (Any, Any, List[Any]) = {
    (list.head, list.tail.head, list.tail.tail)
  }

}
