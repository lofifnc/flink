package org.apache.flink.streaming.test.tool.util

import java.io.IOException

import com.google.common.collect.Iterables
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import java.util.{List => JList}
import java.lang.{Long => JLong}
import java.lang.{Iterable => JIterable}

object Util {

  def splitList[T](input: JList[T], num: Int, numPartitions: Int): JList[T] = {
    val split: ArrayBuffer[T] = ArrayBuffer.empty[T]
    var i: Int = num
    while (i < input.size) {
      split.add(input.get(i))
      i += numPartitions
    }
    split
  }

  def calculateWatermarks[T](records: java.lang.Iterable[StreamRecord[T]]): JList[JLong] = {
    val timestamps = records.map(_.getTimestamp)
    if (timestamps.size != records.size) {
      throw new IOException("The list of watermarks has not the same length as the output")
    }
    insertWatermarks(timestamps.toList)
  }

  implicit def toLongList(lst: List[Long]): JList[JLong] =
    seqAsJavaList(lst.map(i => i: java.lang.Long))

  def insertWatermarks(timestamps: List[Long]): List[Long] = {
    val max = timestamps.max
    val array = new ArrayBuffer[Long]()
    val seen = new ArrayBuffer[Long]()

    def recur(l: List[Long]): List[Long] = {
      if (l.isEmpty) {
        return l
      }
      val ts = l.head
      if (l.count(ts >= _) == 1) {
        val watermark =
          if (l.tail.nonEmpty) {
            val wm = Try(seen.filter(_ < l.tail.min).max)
              .getOrElse(ts)
            if (ts >= wm) {
              ts
            } else {
              wm
            }
          } else {
            max
          }
        array += watermark
      } else {
        array += -1
      }
      seen += ts
      recur(l.tail)
    }
    recur(timestamps)
    array.toList
  }

}
