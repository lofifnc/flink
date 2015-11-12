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

package org.apache.flink.streaming.test.tool.runtime.messaging;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.test.tool.output.OutputVerifier;
import org.apache.flink.streaming.test.tool.runtime.StreamTestFailedException;
import org.apache.flink.streaming.test.tool.util.SerializeUtil;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Opens a 0MQ context and listens for output coming from a sink.
 * Calls the {@link OutputVerifier} with the output.
 *
 * @param <OUT> input type of the sink
 */
public class OutputListener<OUT> implements Callable<Boolean> {

	/**Port 0MQ listens to.*/
	private final int port;
	/** Verifier provided with output */
	private final OutputVerifier<OUT> verifier;
	/** Number of parallel instances */
	private int parallelism = -1;
	/** Set of parallel sink instances that have been started */
	private final Set<Integer> participatingSinks = new HashSet<>();
	/** Set of finished parallel sink instances */
	private final Set<Integer> finishedSinks = new HashSet<>();
	/** Serializer to use for output */
	TypeSerializer<OUT> typeSerializer = null;
	/** Number of records received */
	private int numRecords = 0;
	/** Maximum of records to receive */
	private final long maxElements;

	public OutputListener(int port, OutputVerifier<OUT> verifier, long maxElements) {
		this.port = port;
		this.verifier = verifier;
		this.maxElements = maxElements;
	}


	/**
	 * Listens for output from the test sink.
	 * If the sink is running in parallel
	 *
	 * @return If the test terminated regularly true else false
	 * @throws StreamTestFailedException
	 */
	public Boolean call() throws StreamTestFailedException {
		ZMQ.Context context = ZMQ.context(1);
		// socket to receive from sink
		ZMQ.Socket subscriber = context.socket(ZMQ.PULL);
		subscriber.bind("tcp://*:" + port);

		Action nextStep = Action.ABORT;
		// receive output from sink until finished all sinks are finished
		try {
			nextStep = processMessage(subscriber.recv());
			while (nextStep == Action.CONTINUE) {
				nextStep = processMessage(subscriber.recv());
				//check if test is stopped
				if (Thread.currentThread().isInterrupted()) {
					break;
				}
			}
		}catch (IOException e) {
			subscriber.close();
			context.close();
			verifier.finish();
			return true;
		}
		// close the connection
		subscriber.close();
		context.close();
		verifier.finish();
		// if the last action was not FINISH return false
		return nextStep == Action.FINISH;
	}

	private enum Action {
		CONTINUE,ABORT,FINISH
	}

	private Action processMessage(byte[] bytes)
			throws IOException, StreamTestFailedException {

		MessageType type = MessageType.getMessageType(bytes);
		String msg;
		byte[] out;

		switch (type) {
			case START:
				if (participatingSinks.isEmpty()) {
					verifier.init();
				}
				msg = new String(bytes);
				String[] values = msg.split(" ");
				participatingSinks.add(Integer.parseInt(values[1]));
				parallelism = Integer.parseInt(values[2]);

				break;
			case SER:
				if (typeSerializer == null) {
					out = type.getPayload(bytes);
					typeSerializer = SerializeUtil.deserialize(out);
				}
				break;
			case ELEM:
				out = type.getPayload(bytes);
				OUT elem = SerializeUtil.deserialize(out, typeSerializer);
				numRecords++;
				verifier.receive(elem);
				//
				if(numRecords >= maxElements) {
					return Action.ABORT;
				}
				break;
			case END:
				msg = new String(bytes);
				int sinkIndex = Integer.parseInt(msg.split(" ")[1]);
				finishedSinks.add(sinkIndex);
				break;
		}
		if (finishedSinks.size() >= parallelism) {
			if (participatingSinks.size() != parallelism) {
				throw new IOException("not all parallel sinks have been initialized");
			}
			return Action.FINISH;
		}
		return Action.CONTINUE;
	}
}


