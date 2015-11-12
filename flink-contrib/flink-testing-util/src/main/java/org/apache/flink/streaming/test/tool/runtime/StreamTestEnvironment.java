/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.runtime;


import com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.client.JobTimeoutException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.test.tool.input.EventTimeInput;
import org.apache.flink.streaming.test.tool.input.Input;
import org.apache.flink.streaming.test.tool.input.ParallelFromStreamRecordsFunction;
import org.apache.flink.streaming.test.tool.output.OutputVerifier;
import org.apache.flink.streaming.test.tool.output.TestSink;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

public class StreamTestEnvironment extends TestStreamEnvironment {

	private final ForkableFlinkMiniCluster cluster;

	/**
	 * list of registered handlers to receive the output
	 */
	private List<OutputHandler> handler;

	/**
	 * Flag indicating whether the env has been shutdown forcefully
	 */
	private Boolean stopped = false;

	/**
	 * time in milliseconds before the test gets stopped with a timeout
	 */
	private long timeout = 5000;

	/**
	 * Task to timeout the test execution
	 */
	private TimerTask stopExecution;

	/**
	 * the current port used for transmitting the output via zeroMQ
	 */
	private Integer currentPort;

	public StreamTestEnvironment(ForkableFlinkMiniCluster cluster, int parallelism) {
		super(cluster, parallelism);
		this.cluster = cluster;
		handler = new ArrayList<>();
		currentPort = 5555;
		stopExecution = new TimerTask() {
			public void run() {
				stopExecution();
			}
		};
	}

	/**
	 * Factory method to create a new instance using a provided {@link ForkableFlinkMiniCluster}
	 *
	 * @param parallelism
	 * @return
	 * @throws Exception
	 */
	public static StreamTestEnvironment createTestEnvironment(int parallelism) throws Exception {
		ForkableFlinkMiniCluster cluster =
				TestBaseUtils.startCluster(
						1,
						parallelism,
						StreamingMode.STREAMING,
						false,
						false,
						true
				);
		return new StreamTestEnvironment(cluster, 1);
	}

	/**
	 * Stop the execution of the test.
	 */
	public void stopExecution() {
		stopped = true;
		cluster.shutdown();
	}

	/**
	 * Starts the test execution. And collects the results.
	 *
	 * @throws Throwable any Exception that has occurred
	 *                   during validation the test.
	 */
	public void executeTest() throws Throwable {
		Timer stopTimer = new Timer();
		stopTimer.schedule(stopExecution, timeout);
		try {
			super.execute();
		} catch (JobTimeoutException e) {
			//cluster has been shutdown forcefully
			//most likely by at timeout.
			stopped = true;
		}
		try {
			for (OutputHandler v : handler) {
				v.getTestResult();
			}
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	/**
	 * Creates a TestSink to verify your job output.
	 *
	 * @param verifier which will be used to verify the received records
	 * @param <IN>     type of the input
	 * @return the created sink.
	 */
	public <IN> TestSink<IN> createTestSink(OutputVerifier<IN> verifier) {
		handler.add(new OutputHandler<IN>(currentPort, verifier));
		TestSink<IN> sink = new TestSink<IN>(currentPort);
		currentPort++;
		return sink;
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

	public <OUT> DataStreamSource<OUT> fromInput(EventTimeInput<OUT> input) {
		return fromCollectionWithTimestamp(input.getInput());
	}

	public <OUT> DataStreamSource<OUT> fromInput(Input<OUT> input) {
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
			function = new ParallelFromStreamRecordsFunction<OUT>(typeInfo.createSerializer(getConfig()), data);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		return addSource(function, "Collection Source", outType).setParallelism(1);
	}

	/**
	 * This method can be used to check if the environment has been
	 * stopped prematurely by e.g. a timeout.
	 *
	 * @return true if has been stopped forcefully.
	 */
	public Boolean hasBeenStopped() {
		return stopped;
	}

	/**
	 * Getter for the timeout interval
	 * after the test execution gets stopped.
	 * @return timeout in milliseconds
	 */
	public Long getTimeoutInterval() {
		return timeout;
	}

	/**
	 * Setter for the timeout interval
	 * after the test execution gets stopped.
	 * @param interval
	 */
	public void setTimeoutInterval(long interval) {
		timeout = interval;
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
//     * @see #fromCollection(java.org.apache.flink.streaming.util.Iterator, org.apache.flink.org.apache.flink.streaming.api.common.typeinfo.TypeInformation)
//     */
//    public <OUT> DataStreamSource<OUT> fromCollection(Iterator<StreamRecord<OUT>> data, Class<StreamRecord<OUT>> type) {
//        return fromCollectionWithTimestamp(data, TypeExtractor.getForClass(type));
//    }
}
