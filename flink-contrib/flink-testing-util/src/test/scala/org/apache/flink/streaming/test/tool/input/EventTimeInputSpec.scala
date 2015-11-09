package org.apache.flink.streaming.test.tool.input

import java.util

import org.apache.flink.streaming.runtime.streamrecord
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.test.tool.CoreSpec
import scala.collection.JavaConversions._

class EventTimeInputSpec extends CoreSpec {

  class TestEventTimeInput(input: List[Int]) extends EventTimeInput[Int] {
    override def getInput: util.List[StreamRecord[Int]] =
      input.map(new streamrecord.StreamRecord[Int](_, 0))
  }

  def recordsToInt(lst: Iterable[StreamRecord[Int]]): List[Int] = {
    lst.map(_.getValue).toList
  }

  "The class" should "split the input correctly in 1 partition" in {
    val eti = new TestEventTimeInput(List(1, 2, 3, 4, 5, 6, 7, 8))
    recordsToInt(eti.getSplit(1, 1)) should contain only(1, 2, 3, 4, 5, 6, 7, 8)
  }

  "The class" should "split the input correctly in 2 partitions" in {
    val eti = new TestEventTimeInput(List(1, 2, 3, 4, 5, 6, 7, 8))
    recordsToInt(eti.getSplit(1, 2)) should contain only(1, 3, 5, 7)
    recordsToInt(eti.getSplit(2, 2)) should contain only(2, 4, 6, 8)
  }


  "The class" should "split the input correctly in 3 partitions" in {
    val eti = new TestEventTimeInput(List(1, 2, 3, 4, 5, 6, 7, 8))
    recordsToInt(eti.getSplit(1, 3)) should contain only(1, 4, 7)
    recordsToInt(eti.getSplit(2, 3)) should contain only(2, 5, 8)
    recordsToInt(eti.getSplit(3, 3)) should contain only(3, 6)
  }

  "The class" should "split the input correctly in 4 partitions" in {
    val eti = new TestEventTimeInput(List(1, 2, 3, 4, 5, 6, 7, 8))
    recordsToInt(eti.getSplit(1, 4)) should contain only(1, 5)
    recordsToInt(eti.getSplit(2, 4)) should contain only(2, 6)
    recordsToInt(eti.getSplit(3, 4)) should contain only(3, 7)
    recordsToInt(eti.getSplit(4, 4)) should contain only(4, 8)
  }

}
