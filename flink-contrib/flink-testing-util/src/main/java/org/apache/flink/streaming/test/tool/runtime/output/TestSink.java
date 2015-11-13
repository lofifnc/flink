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

package org.apache.flink.streaming.test.tool.runtime.output;

import com.google.common.primitives.Bytes;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.test.tool.util.SerializeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import java.io.IOException;

/**
 * Provides a sink that sends all incoming records using a 0MQ connection.
 * Starts sending data as soon as the first record is received.
 * Signals the end of data when the sink is closed.
 *
 * @param <IN> record type
 */
public class TestSink<IN> extends RichSinkFunction<IN> {

	private Logger LOG = LoggerFactory.getLogger(RichSinkFunction.class);

	private transient Context context;
	private transient ZMQ.Socket publisher;
	private TypeSerializer<IN> serializer;
	private int port;

	public TestSink(int port) {
		this.port = port;
	}


	@Override
	public void open(Configuration configuration) {
		String jobManagerAddress = configuration
				.getString("jobmanager.rpc.address", "localhost");
		//open a socket to push data
		context = ZMQ.context(1);
		publisher = context.socket(ZMQ.PUSH);
		publisher.connect("tcp://" + jobManagerAddress + ":" + port);

	}

	/**
	 * Called when new data arrives at the sink.
	 * Forwards the records via the 0MQ publisher.
	 *
	 * @param next incoming records
	 */
	@Override
	public void invoke(IN next) {

		int numberOfSubTasks = getRuntimeContext().getNumberOfParallelSubtasks();
		int indexofThisSubTask = getRuntimeContext().getIndexOfThisSubtask();
		byte[] msg;

		if (serializer == null) {

			//transmit parallelism
			publisher.send(String.format("OPEN %d %d",
					indexofThisSubTask,
					numberOfSubTasks), 0);
			//create serializer
			TypeInformation<IN> typeInfo = TypeExtractor.getForObject(next);
			serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
			//push serializer to output receiver
			try {
				msg = Bytes.concat("SER".getBytes(), SerializeUtil.serialize(serializer));
				publisher.send(msg, 0);
			} catch (IOException e) {
				LOG.error("Could not serialize TypeSerializer", e);
				return;
			}
		}


		//serialize input and push to output
		byte[] bytes;
		try {
			bytes = SerializeUtil.serialize(next, serializer);
		} catch (IOException e) {
			LOG.error("Could not serialize input", e);
			return;
		}
		msg = Bytes.concat("REC".getBytes(), bytes);
		publisher.send(msg, 0);
	}

	@Override
	public void close() {
		//signal close to output receiver
		String end = String.format("CLOSE %d",
				getRuntimeContext().getIndexOfThisSubtask());
		publisher.send(end, 0);
		publisher.close();
		context.term();
	}

}
