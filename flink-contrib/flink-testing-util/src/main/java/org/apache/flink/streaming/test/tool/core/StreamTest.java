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

package org.apache.flink.streaming.test.tool.core;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.test.tool.input.EventTimeInput;
import org.apache.flink.streaming.test.tool.input.Input;
import org.apache.flink.streaming.test.tool.runtime.OutputHandler;
import org.apache.flink.streaming.test.tool.output.OutputVerifier;
import org.apache.flink.streaming.test.tool.output.TestSink;
import org.apache.flink.streaming.test.tool.output.assertion.HamcrestVerifier;
import org.apache.flink.streaming.test.tool.output.assertion.OutputMatcher;
import org.apache.flink.streaming.test.tool.runtime.TestingStreamEnvironment;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Offers a base for testing flink streaming applications
 * To use this extend your UnitTest class with this class.
 */
public class StreamTest {

	/** stream environment */
	private TestingStreamEnvironment env;
	/** list of registered handlers to receive the output */
	private List<OutputHandler> handler;
	/** the current port used for transmitting the output via zeroMQ */
	private Integer currentPort;

	@Before
	public void initialize() throws Exception {
		env = TestingStreamEnvironment.createTestEnvironment(2);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		handler = new ArrayList<>();
		currentPort = 5555;
	}

	/**
	 * Creates a DataStreamSource from an EventTimeInput object.
	 * The DataStreamSource will emit the records with the specified EventTime.
	 *
	 * @param input to emit
	 * @param <OUT> type of the emitted records
	 * @return a DataStreamSource generating the input
	 */
	public <OUT> DataStreamSource<OUT> createTestStream(EventTimeInput<OUT> input) {
		return env.fromInput(input);
	}

	/**
	 * Creates a DataStreamSource from an Input object.
	 *
	 * @param input to emit
	 * @param <OUT> type of the emitted records
	 * @return a DataStream generating the input
	 */
	public <OUT> DataStreamSource<OUT> createTestStream(Input<OUT> input) {
		return env.fromInput(input);
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
	 * Creates a TestSink using {@link org.hamcrest.Matcher} to verify the output.
	 *
	 * @param matcher of type Iterable<IN>
	 * @param <IN>
	 * @return the created sink.
	 */
	public <IN> TestSink<IN> createTestSink(org.hamcrest.Matcher<Iterable<IN>> matcher) {
		OutputVerifier<IN> verifier = new HamcrestVerifier<>(matcher);
		return createTestSink(verifier);
	}

	public <T> void matchStream(DataStream<T> stream, OutputMatcher<T> matcher) {
		stream.addSink(createTestSink(matcher));
	}

	public void setParallelism(int n) {
		env.setParallelism(n);
	}

	/**
	 * Executes the test and verifies the output received by the sinks
	 */
	@After
	public void executeTest() throws Throwable {

		env.execute();
		for (OutputHandler v : handler) {
			try {
				v.getTestResult();
			} catch (ExecutionException e) {
				throw e.getCause();
			}
		}

	}

}
