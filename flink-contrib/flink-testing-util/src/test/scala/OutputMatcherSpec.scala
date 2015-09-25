
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.datastream.{DataStreamSink, DataStream, DataStreamSource}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.TestStreamEnvironment
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import output.{TestSink, OutputMatcher}
import runtime.TestingStreamEnvironment
import test.Test
import scala.collection.JavaConversions._

class OutputMatcherSpec extends CoreSpec {

  "The OutputMatcher" should "match allInOrder" in {
    val matcher = new OutputMatcher[Int]()

    val expected = List(1, 2, 3)
    matcher.addExpectedOutput(expected)
    matcher.onlyInOrder()
    matcher.check(expected)
  }

  it should "not match allinOrder" in {
    val matcher = new OutputMatcher[Int]()

    val expected = List(1, 2, 3)
    val actual = List(1 ,3,2 )

    matcher.addExpectedOutput(expected)
    matcher.onlyInOrder()
    matcher.check(actual)
  }

  it should "match in order" in {
    val matcher = new OutputMatcher[Int]()

    val expected = List(1,2,3)
    val actual = List(1,3,2)

    matcher.addExpectedOutput(expected)
    matcher.inOrder(0,1,2)
    matcher.inOrder(0,2)
    matcher.check(actual)
  }

  it should "match all and inOrder" in {
    val matcher = new OutputMatcher[Int]()

    val expected = List(1,2,3)
    val actual = List(1,3,2)

    matcher.addExpectedOutput(expected)
    matcher.all()
    matcher.inOrder(0,1)
    matcher.check(actual)
  }


}
