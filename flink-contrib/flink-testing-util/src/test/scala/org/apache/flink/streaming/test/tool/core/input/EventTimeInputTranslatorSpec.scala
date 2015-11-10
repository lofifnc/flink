package org.apache.flink.streaming.test.tool.core.input

import org.apache.flink.streaming.test.tool.CoreSpec
import org.apache.flink.streaming.test.tool.input.EventTimeInput

import scala.collection.JavaConversions._

class EventTimeInputTranslatorSpec extends CoreSpec{

  class StringToInt(input: EventTimeInput[String])
    extends EventTimeInputTranslator[String, Integer](input) {
    override def translate(elem: String): Integer = Integer.parseInt(elem)
  }

  "The translator" should "transform string input into integer input" in {
    val input = new TestEventTimeInput(List("1","2","3","4","5"))
    recordsToValues(new StringToInt(input).getInput) should contain only(1,2,3,4,5)
  }

}
