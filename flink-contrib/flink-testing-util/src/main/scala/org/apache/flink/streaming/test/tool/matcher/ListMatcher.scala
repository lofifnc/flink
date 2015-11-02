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

import org.hamcrest.{TypeSafeMatcher, Matcher}
import org.scalatest.exceptions.TestFailedException

/**
 * Blueprint for a matcher that checks if a list of objects fulfills
 * a certain requirement.
 * @tparam T
 */
abstract class ListMatcher[T] (val right: List[T]) {

  /**
   * Checks if the list matches the expectations.
   * @throws TestFailedException if the predicate does not match.
   */
  @throws (classOf[TestFailedException])
  def matches(left: List[T])

}
