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

package org.apache.flink.streaming.test.tool;

import org.apache.flink.streaming.test.tool.api.StreamTest;
import org.apache.flink.streaming.test.tool.api.input.After;
import org.apache.flink.streaming.test.tool.api.input.EventTimeInputBuilder;
import org.apache.flink.streaming.test.tool.api.output.ExpectedOutput;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;


public class Test extends StreamTest {

	@org.junit.Test
	public void testWindowing() throws Exception {

		//-------------- org.apache.flink.streaming.test.input
		EventTimeInputBuilder<Tuple2<Integer, String>> input = EventTimeInputBuilder
				.create(new Tuple2<>(1, "test"))
				.add(new Tuple2<>(2, "foo"), After.period(10, TimeUnit.SECONDS))
				.add(new Tuple2<>(3, "bar"), After.period(10, TimeUnit.SECONDS));

		//-------------- expected output
		ExpectedOutput<Tuple2<Integer, String>> expectedOutput = new ExpectedOutput<Tuple2<Integer, String>>()
				.add(new Tuple2<>(3, "test"))
				.add(new Tuple2<>(3, "bar"));
		expectedOutput.expect();


		//------- pipeline definition
		DataStream<Tuple2<Integer, String>> stream = createDataSource(input);

		stream
				.timeWindowAll(Time.of(20, TimeUnit.SECONDS))
				.sum(0)
				.addSink(createTestSink(expectedOutput));

	}

	@org.junit.Test
	public void testMap() throws Exception {

		//-------------- input
		EventTimeInputBuilder<Tuple2<Integer, String>> input = EventTimeInputBuilder
				.create(new Tuple2<>(1, "test"))
				.add(new Tuple2<>(2, "foo"), After.period(10, TimeUnit.SECONDS))
				.add(new Tuple2<>(3, "bar"), After.period(10, TimeUnit.SECONDS));

		//-------------- expected output
		ExpectedOutput<Tuple2<String, Integer>> expectedOutput = new ExpectedOutput<Tuple2<String, Integer>>()
				.add(new Tuple2<>("test", 1))
				.add(new Tuple2<>("foo", 2))
				.add(new Tuple2<>("bar", 3));
		expectedOutput.expect().inOrder().all();
		//------- pipeline definition
		DataStream<Tuple2<Integer, String>> stream = createDataSource(input);

		stream
				.map(new MapFunction<Tuple2<Integer, String>, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> map(Tuple2<Integer, String> input) throws Exception {
						return input.swap();
					}
				})

				.addSink(createTestSink(expectedOutput));

	}
}
