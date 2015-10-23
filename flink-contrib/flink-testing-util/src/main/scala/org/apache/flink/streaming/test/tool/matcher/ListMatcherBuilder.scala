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

import org.apache.flink.streaming.test.tool.matcher.partial.OrderMatcher
import org.scalatest.exceptions.TestFailedException

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * This class is used to define expectations to verify test output.
 * The MatcherBuilder generates a list of [[ListMatcher]]'s which it will
 * use to check the output.
 * @tparam T
 */
class ListMatcherBuilder[T](right: List[T]) extends ListMatcher[T](right){

  /** List of [[ListMatcher]]s to define expectations  */
  private val constraints: ArrayBuffer[ListMatcher[T]] = new ArrayBuffer[ListMatcher[T]]()

  def this(jList: java.util.List[T]) {
    this(jList.toList)
  }

  /*
    END OF CONSTRUCTOR
   */

//  def all() : MatcherBuilder[T] = {
//    constraints += ListMatchers.containsAll[T](right)
//    this
//  }

  /**
   * Tests whether the output contains only the expected records.
   */
  def only() = {
    constraints += ListMatchers.containsOnly[T](right)
    this
  }

  /**
   * Tests whether the output contains no duplicates in reference
   * to the expected output.
   */
  def noDuplicates() = {
    constraints += ListMatchers.containsNoDuplicates[T](right)
    this
  }

  /**
   * Provides a matcher to verify the order of 
   * elements in the output.
   */
  def inOrder() : OrderMatcher[T] = {
    new OrderMatcher[T](constraints,right)
  }

  /**
   * Tests whether the list matches the expectations.
   * @param output actual output.
   */
  def verify(output: java.util.List[T]): Unit = {
    matches(output.toList)
  }

  /**
   * Getter for the list of contraints.
   * @return array of [[ListMatcher]]
   */
  def getConstraints : ArrayBuffer[ListMatcher[T]] = {
    constraints
  }

  /**
   * Checks if the list matches expectation.
   * @param left actual output.
   * @throws TestFailedException if the predicate does not match.
   */
  override def matches(left: scala.List[T]): Unit = {
    if (constraints.nonEmpty) {
      constraints
        .foreach(_.matches(left))
    }else{
      ListMatchers.containsAll[T](right).matches(left)
    }
  }
}



