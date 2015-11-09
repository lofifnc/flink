package org.apache.flink.streaming.test.tool.input;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichEventTimeSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.test.tool.util.SerializeUtil;
import org.apache.flink.streaming.test.tool.util.Util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ParallellFromStreamRecordsFunction<T> extends RichEventTimeSourceFunction<T>
		implements CheckpointedAsynchronously<Integer>{

	private static final long serialVersionUID = 1L;

	/** The (de)serializer to be used for the data elements */
	private final TypeSerializer<StreamRecord<T>> serializer;

	/** The actual data elements, in serialized form */
	private final byte[] elementsSerialized;

	/** The number of serialized elements */
	private final int numElements;

	/** The number of elements emitted already */
	private volatile int numElementsEmitted;

	/** The number of elements to skip initially */
	private volatile int numElementsToSkip;

	/** Flag to make the source cancelable */
	private volatile boolean isRunning = true;

	/** List of watermarks to emit */
	private final List<Long> watermarkList;

	public ParallellFromStreamRecordsFunction(TypeSerializer<StreamRecord<T>> serializer,
											  EventTimeInput<T> input) throws IOException {
		int numberOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();
		int indexofThisSubTask = getRuntimeContext().getIndexOfThisSubtask();

		Iterable<StreamRecord<T>> inputSplit = input.getSplit(indexofThisSubTask, numberOfSubTasks);

		if(numberOfSubTasks > input.getMaximumNumberOfSplits()) {
			throw new IllegalStateException("Input arallelism is higher than the" +
					" maximum number of parallel sources");
		}

		watermarkList = Util.calculateWatermarks(inputSplit);
		if(watermarkList.size() != Iterables.size(inputSplit)) {
			throw new IOException("The list of watermarks has not the same length as the output");
		}

		this.serializer = serializer;
		elementsSerialized = SerializeUtil.serializeOutput(inputSplit,serializer).toByteArray();
		numElements = Iterables.size(inputSplit);
	}


	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
		final DataInputView input = new InputViewDataInputStreamWrapper(new DataInputStream(bais));
		Iterator<Long> watermarks = watermarkList.iterator();

		// if we are restored from a checkpoint and need to skip elements, skip them now.
		int toSkip = numElementsToSkip;
		if (toSkip > 0) {
			try {
				while (toSkip > 0) {
					serializer.deserialize(input);
					watermarks.next();
					toSkip--;
				}
			}
			catch (Exception e) {
				throw new IOException("Failed to deserialize an element from the source. " +
						"If you are using user-defined serialization (Value and Writable types), check the " +
						"serialization functions.\nSerializer is " + serializer);
			}

			this.numElementsEmitted = this.numElementsToSkip;
		}

		final Object lock = ctx.getCheckpointLock();

		while (isRunning && numElementsEmitted < numElements) {
			StreamRecord<T> next;
			try {
				next = serializer.deserialize(input);
			}
			catch (Exception e) {
				throw new IOException("Failed to deserialize an element from the source. " +
						"If you are using user-defined serialization (Value and Writable types), check the " +
						"serialization functions.\nSerializer is " + serializer);
			}

			synchronized (lock) {
				//logic where to put?
				ctx.collectWithTimestamp(next.getValue(), next.getTimestamp());
				Long watermark = watermarks.next();
				if(watermark > 0) {
					ctx.emitWatermark(new Watermark(watermark));
				}
				numElementsEmitted++;
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	/**
	 * Gets the number of elements produced in total by this function.
	 *
	 * @return The number of elements produced in total.
	 */
	public int getNumElements() {
		return numElements;
	}

	/**
	 * Gets the number of elements emitted so far.
	 *
	 * @return The number of elements emitted so far.
	 */
	public int getNumElementsEmitted() {
		return numElementsEmitted;
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public Integer snapshotState(long checkpointId, long checkpointTimestamp) {
		return this.numElementsEmitted;
	}

	@Override
	public void restoreState(Integer state) {
		this.numElementsToSkip = state;
	}

	public static <OUT> void checkCollection(Iterable<OUT> elements, Class<OUT> viewedAs) {
		Iterator i$ = elements.iterator();

		Object elem;
		do {
			if(!i$.hasNext()) {
				return;
			}

			elem = i$.next();
			if(elem == null) {
				throw new IllegalArgumentException("The collection contains a null element");
			}
		} while(viewedAs.isAssignableFrom(elem.getClass()));

		throw new IllegalArgumentException("The elements in the collection are not all subclasses of " + viewedAs.getCanonicalName());
	}
}
