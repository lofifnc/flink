package runtime;


import com.google.common.base.Preconditions;
import input.EventTimeInput;
import input.FromEventTimeElementsFunction;
import input.Input;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class TestingStreamEnvironment extends TestStreamEnvironment {

	public TestingStreamEnvironment(int parallelism, long memorySize) {
		super(parallelism, memorySize);
	}

	public TestingStreamEnvironment(ForkableFlinkMiniCluster executor, int parallelism) {
		super(executor, parallelism);
	}

	/**
	 * Creates a new data stream that contains the given elements. The elements must all be of the same type, for
	 * example, all of the {@link String} or {@link Integer}.
	 * <p>
	 * The framework will try and determine the exact type from the elements. In case of generic elements, it may be
	 * necessary to manually supply the type information via {@link #fromCollection(java.util.Collection,
	 * org.apache.flink.api.common.typeinfo.TypeInformation)}.
	 * <p>
	 * Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * degree of parallelism one.
	 *
	 * @param data  The array of elements to create the data stream from.
	 * @param <OUT> The type of the returned data stream
	 * @return The data stream representing the given array of elements
	 */
	@SafeVarargs
	public final <OUT> DataStreamSource<OUT> fromElementsWithTimeStamp(StreamRecord<OUT>... data) {
		if (data.length == 0) {
			throw new IllegalArgumentException("fromElements needs at least one element as argument");
		}

		TypeInformation<OUT> typeInfo;
		try {
			typeInfo = TypeExtractor.getForObject(data[0].getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + data[0].getClass().getName()
					+ "; please specify the TypeInformation manually via "
					+ "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
		}
		return fromCollectionWithTimestamp(Arrays.asList(data), typeInfo);
	}

	public <OUT> DataStreamSource<OUT> fromInput(EventTimeInput<OUT> input){
		return fromCollectionWithTimestamp(input.getInput());
	}

	public <OUT> DataStreamSource<OUT> fromInput(Input<OUT> input){
		return fromCollection(input.getInput());
	}

	/**
	 * Creates a data stream from the given non-empty collection. The type of the data stream is that of the
	 * elements in the collection.
	 * <p>
	 * <p>The framework will try and determine the exact type from the collection elements. In case of generic
	 * elements, it may be necessary to manually supply the type information via
	 * {@link #fromCollection(java.util.Collection, org.apache.flink.api.common.typeinfo.TypeInformation)}.</p>
	 * <p>
	 * <p>Note that this operation will result in a non-parallel data stream source, i.e. a data stream source with a
	 * parallelism one.</p>
	 *
	 * @param data  The collection of elements to create the data stream from.
	 * @param <OUT> The generic type of the returned data stream.
	 * @return The data stream representing the given collection
	 */
	public <OUT> DataStreamSource<OUT> fromCollectionWithTimestamp(Collection<StreamRecord<OUT>> data) {
		Preconditions.checkNotNull(data, "Collection must not be null");
		if (data.isEmpty()) {
			throw new IllegalArgumentException("Collection must not be empty");
		}

		StreamRecord<OUT> first = data.iterator().next();
		if (first == null) {
			throw new IllegalArgumentException("Collection must not contain null elements");
		}

		TypeInformation<OUT> typeInfo;
		try {
			typeInfo = TypeExtractor.getForObject(first.getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + first.getClass()
					+ "; please specify the TypeInformation manually via "
					+ "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
		}
		return fromCollectionWithTimestamp(data, typeInfo);
	}

	/**
	 * Creates a data stream from the given non-empty collection.
	 * <p>
	 * <p>Note that this operation will result in a non-parallel data stream source,
	 * i.e., a data stream source with a parallelism one.</p>
	 *
	 * @param data    The collection of elements to create the data stream from
	 * @param outType The TypeInformation for the produced data stream
	 * @param <OUT>   The type of the returned data stream
	 * @return The data stream representing the given collection
	 */
	public <OUT> DataStreamSource<OUT> fromCollectionWithTimestamp(Collection<StreamRecord<OUT>> data,
																TypeInformation<OUT> outType) {
		Preconditions.checkNotNull(data, "Collection must not be null");

		TypeInformation<StreamRecord<OUT>> typeInfo;
		StreamRecord<OUT> first = data.iterator().next();
		try {
			typeInfo = TypeExtractor.getForObject(first);
		} catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + first.getClass()
					+ "; please specify the TypeInformation manually via "
					+ "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
		}

		// must not have null elements and mixed elements
		FromElementsFunction.checkCollection(data, typeInfo.getTypeClass());

		SourceFunction<OUT> function;
		try {
			function = new FromEventTimeElementsFunction<OUT>(typeInfo.createSerializer(getConfig()), data);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		return addSource(function, "Collection Source", outType).setParallelism(1);
	}

//    /**
//     * Creates a data stream from the given iterator.
//     *
//     * <p>Because the iterator will remain unmodified until the actual execution happens,
//     * the type of data returned by the iterator must be given explicitly in the form of the type
//     * class (this is due to the fact that the Java compiler erases the generic type information).</p>
//     *
//     * <p>Note that this operation will result in a non-parallel data stream source, i.e.,
//     * a data stream source with a parallelism of one.</p>
//     *
//     * @param data
//     * 		The iterator of elements to create the data stream from
//     * @param type
//     * 		The class of the data produced by the iterator. Must not be a generic class.
//     * @param <OUT>
//     * 		The type of the returned data stream
//     * @return The data stream representing the elements in the iterator
//     * @see #fromCollection(java.util.Iterator, org.apache.flink.api.common.typeinfo.TypeInformation)
//     */
//    public <OUT> DataStreamSource<OUT> fromCollection(Iterator<StreamRecord<OUT>> data, Class<StreamRecord<OUT>> type) {
//        return fromCollectionWithTimestamp(data, TypeExtractor.getForClass(type));
//    }
}
