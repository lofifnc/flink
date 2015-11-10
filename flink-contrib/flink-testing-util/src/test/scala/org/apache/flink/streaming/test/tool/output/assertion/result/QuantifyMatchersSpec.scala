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
package org.apache.flink.streaming.test.tool.output.assertion.result

import org.apache.flink.streaming.test.tool.CoreSpec
import org.hamcrest.Matchers

import scala.collection.JavaConverters._

class QuantifyMatchersSpec extends CoreSpec {

  //TODO make this test better readable
  val list = List(1,2,3,4,5,6,7,8)

  "The matcher" should "implement any" in {
    AnyOf.any(Matchers.is(2))
      .matches(list.asJava) should be(true)
    AnyOf.any(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(true)
    AnyOf.any(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(true)
    AnyOf.any(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement each" in {
    EachOf.each(Matchers.is(2))
      .matches(list.asJava) should be(false)
    EachOf.each(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(false)
    EachOf.each(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(true)
    EachOf.each(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement one" in {
    OneOf.one(Matchers.is(2))
      .matches(list.asJava) should be(true)
    OneOf.one(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(false)
    OneOf.one(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(false)
    OneOf.one(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement atLeast" in {
    AtLeast.atLeast(Matchers.is(2),2)
      .matches(list.asJava) should be(false)
    AtLeast.atLeast(Matchers.greaterThan(new Integer(6)),2)
      .matches(list.asJava) should be(true)
    AtLeast.atLeast(Matchers.lessThan(new Integer(12)),2)
      .matches(list.asJava) should be(true)
    AtLeast.atLeast(Matchers.greaterThan(new Integer(12)),2)
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement atMost" in {
    Exactly.exactly(Matchers.is(2),2)
      .matches(list.asJava) should be(false)
    Exactly.exactly(Matchers.greaterThan(new Integer(6)),2)
      .matches(list.asJava) should be(true)
    Exactly.exactly(Matchers.lessThan(new Integer(12)),2)
      .matches(list.asJava) should be(false)
    Exactly.exactly(Matchers.greaterThan(new Integer(12)),2)
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement none" in {
    RecordsMatchers.none(Matchers.is(2))
      .matches(list.asJava) should be(false)
    RecordsMatchers.none(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(false)
    RecordsMatchers.none(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(false)
    RecordsMatchers.none(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(true)
  }


}
