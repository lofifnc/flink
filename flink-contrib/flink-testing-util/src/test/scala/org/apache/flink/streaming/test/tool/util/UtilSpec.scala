package org.apache.flink.streaming.test.tool.util

import java.util

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.test.tool.CoreSpec

import scala.collection.JavaConversions._

class UtilSpec extends CoreSpec {

  "insertWatermarks" should "produce a list of watermarks" in {
    val list: List[Long] = List(3, 1, 11, 2, 5, 4, 10, 8, 7, 9)
    Util.insertWatermarks(list) shouldBe List(-1,1,-1,3,-1,5,-1,-1,8,11)
  }

  "insertWatermarks" should "produce a list of sorted watermarks" in {
    val list: List[Long] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    Util.insertWatermarks(list) shouldBe List(1,2,3,4,5,6,7,8,9,10)
  }

  "insertWatermarks" should "produce one watermark" in {
    val list: List[Long] = List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
    Util.insertWatermarks(list) shouldBe List(-1,-1,-1,-1,-1,-1,-1,-1,-1,10)
  }

  "calculateWatermarks" should "convert a list of streamrecords" in {
    val list = new util.ArrayList[StreamRecord[Integer]]()
    list.add(new StreamRecord[Integer](1,2))
    val result = Util.calculateWatermarks(list)
    result should contain only(2)
  }

}
