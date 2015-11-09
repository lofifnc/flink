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

package org.apache.flink.streaming.test.tool.output;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.test.tool.util.SerializeUtil;
import org.scalatest.exceptions.TestFailedException;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;

/**
 * Handles the org.apache.flink.streaming.test.output coming from a sink
 * and calls the verifier
 *
 * @param <OUT> org.apache.flink.streaming.test.input type of the sink
 */
public class OutputListener<OUT> implements Callable<ArrayList<OUT>> {

	private final int port;
	private final OutputVerifier<OUT> verifier;

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
		// receive serializer
		TypeSerializer<OUT> typeSerializer = SerializeUtil.deserialize(subscriber.recv());
		verifier.init();
		// receive .output from sink until END message;
		byte[] out = subscriber.recv();

		while (!Arrays.equals(out, "END".getBytes())) {
			byteArray.add(out);
			verifier.receive(SerializeUtil.deserialize(out,typeSerializer));
			out = subscriber.recv();
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
}


