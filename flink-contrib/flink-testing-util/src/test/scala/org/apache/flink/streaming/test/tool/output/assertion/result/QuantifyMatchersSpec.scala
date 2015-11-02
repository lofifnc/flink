package org.apache.flink.streaming.test.tool.output.assertion.result

import org.apache.flink.streaming.test.tool.CoreSpec
import org.hamcrest.Matchers
import scala.collection.JavaConverters._

class QuantifyMatchersSpec extends CoreSpec {

  val list = List(1,2,3,4,5,6,7,8)

  "The matcher" should "implement any" in {
    QuantifyMatchers.any(Matchers.is(2))
      .matches(list.asJava) should be(true)
    QuantifyMatchers.any(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(true)
    QuantifyMatchers.any(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(true)
    QuantifyMatchers.any(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement each" in {
    QuantifyMatchers.each(Matchers.is(2))
      .matches(list.asJava) should be(false)
    QuantifyMatchers.each(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(false)
    QuantifyMatchers.each(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(true)
    QuantifyMatchers.each(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement one" in {
    QuantifyMatchers.one(Matchers.is(2))
      .matches(list.asJava) should be(true)
    QuantifyMatchers.one(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(false)
    QuantifyMatchers.one(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(false)
    QuantifyMatchers.one(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement atLeast" in {
    QuantifyMatchers.atLeast(Matchers.is(2),2)
      .matches(list.asJava) should be(false)
    QuantifyMatchers.atLeast(Matchers.greaterThan(new Integer(6)),2)
      .matches(list.asJava) should be(true)
    QuantifyMatchers.atLeast(Matchers.lessThan(new Integer(12)),2)
      .matches(list.asJava) should be(true)
    QuantifyMatchers.atLeast(Matchers.greaterThan(new Integer(12)),2)
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement atMost" in {
    QuantifyMatchers.exactly(Matchers.is(2),2)
      .matches(list.asJava) should be(false)
    QuantifyMatchers.exactly(Matchers.greaterThan(new Integer(6)),2)
      .matches(list.asJava) should be(true)
    QuantifyMatchers.exactly(Matchers.lessThan(new Integer(12)),2)
      .matches(list.asJava) should be(false)
    QuantifyMatchers.exactly(Matchers.greaterThan(new Integer(12)),2)
      .matches(list.asJava) should be(false)
  }

  "The matcher" should "implement none" in {
    QuantifyMatchers.none(Matchers.is(2))
      .matches(list.asJava) should be(false)
    QuantifyMatchers.none(Matchers.greaterThan(new Integer(2)))
      .matches(list.asJava) should be(false)
    QuantifyMatchers.none(Matchers.lessThan(new Integer(12)))
      .matches(list.asJava) should be(false)
    QuantifyMatchers.none(Matchers.greaterThan(new Integer(12)))
      .matches(list.asJava) should be(true)
  }


}
