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
package org.apache.flink.streaming.test.tool.core.assertion.tuple

import org.apache.flink.api.java.tuple.{Tuple3 => Fluple3}
import org.apache.flink.streaming.test.tool.CoreSpec
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair
import org.apache.flink.streaming.test.tool.core.output.TupleMask
import org.hamcrest.Matchers

import scala.collection.JavaConverters._

class TupleMapMatchersSpec extends CoreSpec {

  trait TupleMapMatchersCase {
    val mask = new TupleMask[Fluple3[Int, Int, Int]]("one", "two", "three")
    val matchers: List[KeyMatcherPair] = List(
      KeyMatcherPair.of("one", Matchers.is(1)),
      KeyMatcherPair.of("two", Matchers.is(1)),
      KeyMatcherPair.of("three", Matchers.is(1))
    )
  }

  "The any matcher" should "implement any" in new TupleMapMatchersCase {
    val matcher = Any.any(matchers.asJava, mask)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe true
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe true
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe true
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false
  }

  "The each matcher" should "implement each" in new TupleMapMatchersCase {
    val matcher = Each.each(matchers.asJava, mask)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe true
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe false
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe false
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false

  }

  "The one matcher" should "implement one" in new TupleMapMatchersCase {
    val matcher = One.one(matchers.asJava, mask)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe false
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe false
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe true
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false
  }

  "The matcher" should "implement exactly" in new TupleMapMatchersCase {
    val matcher = Exactly.exactly(matchers.asJava, mask, 2)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe false
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe true
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe false
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false

  }

  "The matcher" should "implement atMost" in new TupleMapMatchersCase {
    val matcher = AtMost.atMost(matchers.asJava, mask, 2)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe false
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe true
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe true
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe true

  }
  
  "The matcher" should "implement atLeast" in new TupleMapMatchersCase {
    val matcher = AtLeast.atLeast(matchers.asJava, mask, 2)

    matcher.matches(Fluple3.of(1, 1, 1)) shouldBe true
    matcher.matches(Fluple3.of(1, 1, 0)) shouldBe true
    matcher.matches(Fluple3.of(1, 0, 0)) shouldBe false
    matcher.matches(Fluple3.of(0, 0, 0)) shouldBe false

  }

}
