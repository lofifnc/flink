package input;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * A stream source function that returns a sequence of elements.
 *
 * <p>Upon construction, this source function serializes the elements using Flink's type information.
 * That way, any object transport using Java serialization will not be affected by the serializability
 * of the elements.</p>
 *
 * @param <T> The type of elements returned by this function.
 */
public class FromEventTimeElementsFunction<T> implements EventTimeSourceFunction<T>, CheckpointedAsynchronously<Integer> {

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


	public FromEventTimeElementsFunction(TypeSerializer<StreamRecord<T>> serializer, StreamRecord<T>... elements) throws IOException {
		this(serializer, Arrays.asList(elements));
	}

	public FromEventTimeElementsFunction(TypeSerializer<StreamRecord<T>> serializer, Iterable<StreamRecord<T>> elements) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputViewDataOutputStreamWrapper wrapper = new OutputViewDataOutputStreamWrapper(new DataOutputStream(baos));

		int count = 0;
		try {
			for (StreamRecord<T> element : elements) {
				serializer.serialize(element, wrapper);
				count++;
			}
		}
		catch (Exception e) {
			throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
		}

		this.serializer = serializer;
		this.elementsSerialized = baos.toByteArray();
		this.numElements = count;
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
		final DataInputView input = new InputViewDataInputStreamWrapper(new DataInputStream(bais));

		// if we are restored from a checkpoint and need to skip elements, skip them now.
		int toSkip = numElementsToSkip;
		if (toSkip > 0) {
			try {
				while (toSkip > 0) {
					serializer.deserialize(input);
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
				ctx.collectWithTimestamp(next.getValue(), next.getTimestamp());
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


}
