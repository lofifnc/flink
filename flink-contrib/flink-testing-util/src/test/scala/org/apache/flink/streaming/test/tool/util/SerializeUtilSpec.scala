package org.apache.flink.streaming.test.tool.util


import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.test.tool.CoreSpec

class SerializeUtilSpec extends CoreSpec {

  class Person {
    var age: Integer = _
    var name: String = _

  }

  trait SerializeUtilCase {
    val config = new ExecutionConfig()
    val typeInfo : TypeInformation[String] = TypeExtractor.getForObject("test")
    val serializer = typeInfo.createSerializer(config)
  }

  "The util" should "serialize and deserialize a String" in new SerializeUtilCase{
    val string = "test-string"
    val bytes = SerializeUtil.serialize(string,serializer)

    SerializeUtil.deserialize(bytes,serializer) should equal(string)
  }

  "The util" should "serialize and deserialize a [[TypeSerializer]]" in new SerializeUtilCase {
    val bytes = SerializeUtil.serialize(serializer)
    SerializeUtil.deserialize(bytes).asInstanceOf[TypeSerializer[String]] should equal(serializer)
  }

  "The util" should "serialize and deserialize a [[Pair]]" in {
    val tuple = ("right","left")
    val serializer = TypeExtractor
      .getForObject(tuple)
      .createSerializer(new ExecutionConfig)
    val bytes = SerializeUtil.serialize(tuple,serializer)
    SerializeUtil.deserialize(bytes,serializer) should equal(tuple)
  }



}
