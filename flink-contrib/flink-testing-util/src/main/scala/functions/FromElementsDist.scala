package functions

import java.io._
import java.util
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{InputViewDataInputStreamWrapper, DataInputView, OutputViewDataOutputStreamWrapper}
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, EventTimeSourceFunction, SourceFunction}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import scala.collection.JavaConversions._
import org.apache.flink.configuration.Configuration

object FromElementsDist {

  @throws(classOf[IOException])
  def apply[T](serializer: TypeSerializer[StreamRecord[T]],
            elements: Iterable[StreamRecord[T]]): FromElementsDist[T] ={
    val (elementsSerialized, numElements) = {
      val baos = new ByteArrayOutputStream
      val wrapper = new OutputViewDataOutputStreamWrapper(new DataOutputStream(baos))

      var count: Int = 0

      try {
        elements.foreach { element =>
          serializer.serialize(element, wrapper)
          count += 1
        }

        (baos.toByteArray, count)
      }
      catch {
        case e: Exception => {
          throw new IOException("Serializing the source elements failed: " + e.getMessage, e)
        }
      }
    }
    new FromElementsDist[T](serializer,elementsSerialized,numElements)
  }

  @throws(classOf[IOException])
  def apply[T](serializer: TypeSerializer[StreamRecord[T]],
               elements: util.Collection[StreamRecord[T]]): FromElementsDist[T] ={
    FromElementsDist(serializer,elements.toList)
  }

}

class FromElementsDist[T](serializer: TypeSerializer[StreamRecord[T]], elementsSerialized: Array[Byte],numElements : Int)
  extends RichSourceFunction[T]
  with EventTimeSourceFunction[T]
  with CheckpointedAsynchronously[Integer] {

  private val serialVersionUID: Long = 1L
  /** The number of elements emitted already */
  @volatile
  private var numElementsEmitted: Int = 0
  /** The number of elements to skip initially */
  @volatile
  private var numElementsToSkip: Int = 0
  /** Flag to make the source cancelable */
  @volatile
  private var isRunning: Boolean = true

  // ------------------------------------------------------------------------
  //  END OF CONSTRUCTOR
  // ------------------------------------------------------------------------

  @throws(classOf[Exception])
  def run(ctx: SourceFunction.SourceContext[T]) {
    val bais: ByteArrayInputStream = new ByteArrayInputStream(elementsSerialized)
    val input: DataInputView = new InputViewDataInputStreamWrapper(new DataInputStream(bais))
    var toSkip: Int = numElementsToSkip
    if (toSkip > 0) {
      try {
        while (toSkip > 0) {
          serializer.deserialize(input)
          toSkip -= 1
        }
      }
      catch {
        case e: Exception => {
          throw new IOException("Failed to deserialize an element from the source. " + "If you are using user-defined serialization (Value and Writable types), check the " + "serialization functions.\nSerializer is " + serializer)
        }
      }
      this.numElementsEmitted = this.numElementsToSkip
    }
    val lock: AnyRef = ctx.getCheckpointLock
    while (isRunning && numElementsEmitted < numElements) {
      var next: StreamRecord[T] = null
      try {
        next = serializer.deserialize(input)
      }
      catch {
        case e: Exception => {
          throw new IOException("Failed to deserialize an element from the source. " + "If you are using user-defined serialization (Value and Writable types), check the " + "serialization functions.\nSerializer is " + serializer)
        }
      }
      lock synchronized {
        ctx.collectWithTimestamp(next.getValue, next.getTimestamp)
        numElementsEmitted += 1
      }
    }
  }

  override def open(parameters: Configuration): Unit = {

  }

  override def cancel() {
    isRunning = false
  }

  /**
   * Gets the number of elements produced in total by this function.
   *
   * @return The number of elements produced in total.
   */
  def getNumElements: Int = {
    return numElements
  }

  /**
   * Gets the number of elements emitted so far.
   *
   * @return The number of elements emitted so far.
   */
  def getNumElementsEmitted: Int = {
    return numElementsEmitted
  }

  // ------------------------------------------------------------------------
  //  Checkpointing
  // ------------------------------------------------------------------------
  override def snapshotState(checkpointId: Long, checkpointTimestamp: Long): Integer = {
    this.numElementsEmitted
  }

  override def restoreState(state: Integer) {
    this.numElementsToSkip = state
  }
}
