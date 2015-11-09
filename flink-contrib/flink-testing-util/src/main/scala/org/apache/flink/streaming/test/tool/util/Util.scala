package org.apache.flink.streaming.test.tool.util

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import java.util.{List => JList}
import java.lang.{Long => JLong}

object Util {

  def calculateWatermarks[T](records: java.lang.Iterable[StreamRecord[T]]): JList[JLong] = {
    val timestamps = records.map(_.getTimestamp)
    insertWatermarks(timestamps.toList)
  }

  implicit def toLongList( lst: List[Long] ) : JList[JLong] =
    seqAsJavaList( lst.map( i => i:java.lang.Long ) )

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
            if(ts >= wm) {
              ts
            }else{
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
