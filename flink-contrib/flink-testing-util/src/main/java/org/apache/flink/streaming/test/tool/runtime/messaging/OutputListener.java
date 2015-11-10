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
import org.apache.flink.streaming.test.tool.util.SerializeUtil;
import org.scalatest.exceptions.TestFailedException;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Opens a 0MQ context and listens for output coming from a sink.
 * Calls the verifier with the output.
 *
 * @param <OUT> input type of the sink
 */
public class OutputListener<OUT> implements Callable<ArrayList<OUT>> {

	private final int port;
	private final OutputVerifier<OUT> verifier;
	private Set<Integer> participatingSinks = new HashSet<>();
	private Set<Integer> finishedSinks = new HashSet<>();
	private int parallelism = -1;
	TypeSerializer<OUT> typeSerializer = null;

	public OutputListener(int port, OutputVerifier<OUT> verifier) {
		this.port = port;
		this.verifier = verifier;
	}

	public ArrayList<OUT> call() throws IOException, TestFailedException {
		ArrayList<byte[]> byteArray = new ArrayList<byte[]>();
		ZMQ.Context context = ZMQ.context(1);
		// socket to receive from sink
		ZMQ.Socket subscriber = context.socket(ZMQ.PULL);
		subscriber.bind("tcp://*:" + port);

		// receive output from sink until finished all sinks are finished
		boolean isFinished = processMessage(subscriber.recv());
		while (!isFinished) {
			isFinished = processMessage(subscriber.recv());
		}
		// close the connection
		subscriber.close();
		context.close();
		verifier.finish();
		ArrayList<OUT> sinkInput = new ArrayList<>();
		// deserialize messages received from sink
		for (byte[] b : byteArray) {
			sinkInput.add(SerializeUtil.deserialize(b, typeSerializer));
		}
		return sinkInput;
	}

	private Boolean processMessage(byte[] bytes) throws IOException {

		MessageType type = MessageType.getMessageType(bytes);
		String msg;
		byte[] out;

		switch (type) {
			case START:
				if(participatingSinks.isEmpty()) {
					verifier.init();
				}
				msg = new String(bytes);
				String[] values = msg.split(" ");
				participatingSinks.add(Integer.parseInt(values[1]));
				parallelism = Integer.parseInt(values[2]);

				break;
			case SER:
				if(typeSerializer == null) {
					out = type.getPayload(bytes);
					typeSerializer = SerializeUtil.deserialize(out);
				}
				break;
			case ELEM:
				out = type.getPayload(bytes);
				OUT elem = SerializeUtil.deserialize(out,typeSerializer);
				verifier.receive(elem);
				break;
			case END:
				msg = new String(bytes);
				int sinkIndex = Integer.parseInt(msg.split(" ")[1]);
				finishedSinks.add(sinkIndex);
				break;
		}
		if(finishedSinks.size() == parallelism) {
			if(participatingSinks.size() != parallelism) {
				throw new IOException("not all parallel sinks have been initialized");
			}
			return true;
		}
		return false;
	}
}


