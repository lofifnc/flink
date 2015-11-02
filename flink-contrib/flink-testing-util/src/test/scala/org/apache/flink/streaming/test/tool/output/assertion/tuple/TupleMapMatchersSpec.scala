package org.apache.flink.streaming.test.tool.output.assertion.tuple

import org.apache.flink.streaming.test.tool.CoreSpec
import org.apache.flink.streaming.test.tool.core.output.map.OutputTable
import org.hamcrest.{Matcher, Matchers}
import org.apache.flink.api.java.tuple.{Tuple2 => Fluple2}

import scala.collection.JavaConverters._

class TupleMapMatchersSpec extends CoreSpec {

  trait TupleMapMatchersCase {
    val table = new OutputTable[Fluple2[String, Integer]]("name", "age")
    val matchers: List[Fluple2[String, Matcher[_]]] = List(
      new Fluple2("name", Matchers.is("hans")),
      new Fluple2("age", Matchers.is(12))
    )
  }

  "The matcher" should "implement any" in new TupleMapMatchersCase {
    val matcher = TupleMapMatchers.any(matchers.asJava, table)

    matcher.matches(Fluple2.of("hans", 12)) should be(true)
    matcher.matches(Fluple2.of("pete", 12)) should be(true)
    matcher.matches(Fluple2.of("hans", 13)) should be(true)
    matcher.matches(Fluple2.of("pete", 13)) should be(false)
  }

  "The matcher" should "implement each" in new TupleMapMatchersCase {
    val matcher = TupleMapMatchers.each(matchers.asJava, table)

    matcher.matches(Fluple2.of("hans", 12)) should be(true)
    matcher.matches(Fluple2.of("pete", 12)) should be(false)
    matcher.matches(Fluple2.of("hans", 13)) should be(false)
    matcher.matches(Fluple2.of("pete", 13)) should be(false)
  }

}
