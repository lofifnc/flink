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

package org.apache.flink.streaming.test.tool.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.test.tool.core.StreamTest;
import org.apache.flink.streaming.test.tool.core.output.ExpectedOutput;
import org.apache.flink.streaming.test.tool.core.output.map.TupleMask;
import org.apache.flink.streaming.test.tool.output.assertion.AssertBlock;
import org.apache.flink.streaming.test.tool.output.assertion.OutputMatcher;

import static org.apache.flink.streaming.test.tool.core.Sugar.after;
import static org.apache.flink.streaming.test.tool.core.Sugar.fromInput;
import static org.apache.flink.streaming.test.tool.core.Sugar.seconds;
import static org.apache.flink.streaming.test.tool.core.Sugar.times;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;

public class Test extends StreamTest {

	public static DataStream<Tuple2<Integer, String>> window(DataStream<Tuple2<Integer, String>> stream) {
		return stream.timeWindowAll(Time.of(20, seconds)).sum(0);

	}

	public static DataStream<Tuple2<String, Integer>> swap(DataStream<Tuple2<Integer, String>> stream) {
		return stream.map(new MapFunction<Tuple2<Integer, String>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> map(Tuple2<Integer, String> input) throws Exception {
				return input.swap();
			}
		});
	}

	@org.junit.Test
	public void testWindowing() throws Exception {

		TupleMask<Tuple2<Integer,String>> mask = TupleMask.create("value","name");


		//------- input definition
		DataStream<Tuple2<Integer, String>> testStream = createTestStream(
				fromInput(Tuple2.of(1, "test"))
						.emit(Tuple2.of(2, "boo"), after(10, seconds))
						.repeatablyEmit(Tuple2.of(3, "bar"), after(20, seconds), times(10))
						.repeatInput(after(10, seconds), times(1))
		);

		//------- output definition
		OutputMatcher<Tuple2<Integer, String>> matcher =
				AssertBlock.fromMask(mask)
						.assertThat("value", is(3))
						.assertThat("name", either(is("test")).or(is("bar")))
						.anyOfThem().onEachRecord();


		matchStream(window(testStream), matcher);
	}

	@org.junit.Test
	public void testMap() throws Exception {

		//-------------- input
		DataStream<Tuple2<Integer, String>> stream = createTestStream(
				fromInput(Tuple2.of(1, "test"))
						.emit(Tuple2.of(2, "foo"), after(10, seconds))
						.emit(Tuple2.of(3, "bar"), after(10, seconds))
		);

		//-------------- expected output
		ExpectedOutput<Tuple2<String, Integer>> expectedOutput = new ExpectedOutput<Tuple2<String, Integer>>()
				.expect(Tuple2.of("test", 1))
				.expect(Tuple2.of("foo", 2));
		expectedOutput.refine().noDuplicates().inOrder().all();

		matchStream(swap(stream), expectedOutput);

	}
}
