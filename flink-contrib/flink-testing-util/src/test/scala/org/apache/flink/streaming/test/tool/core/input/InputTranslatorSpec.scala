package org.apache.flink.streaming.test.tool.core.input

import org.apache.flink.streaming.test.tool.CoreSpec
import org.apache.flink.streaming.test.tool.input.Input

class InputTranslatorSpec extends CoreSpec{

  class StringToInt(input: Input[String])
    extends InputTranslator[String, Integer](input) {
    override def translate(elem: String): Integer = Integer.parseInt(elem)
  }

  "The translator" should "transform string input into integer input" in {
    val input = new TestInput(List("1","2","3","4","5"))
    new StringToInt(input).getInput should contain only(1,2,3,4,5)
  }
}
