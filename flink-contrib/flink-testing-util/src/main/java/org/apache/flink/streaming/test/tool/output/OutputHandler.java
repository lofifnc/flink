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

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * Registers a zeroMQ context and listens for output from the appropriate sink
 * the getOutput method is used to wait for output
 * @param <OUT> type of data the sink is sending to the listener.
 */
public class OutputHandler<OUT> {

	private ExecutorService executorService = Executors.newSingleThreadExecutor();
	private FutureTask<ArrayList<OUT>> outputFuture;
//	private OutputVerifier<OUT> verifier;

	/**
	 * Starts a listener in the background
	 * @param port
	 */
	public OutputHandler(Integer port, OutputVerifier<OUT> verifier) {
		outputFuture = startListening(port,verifier);
	}


	private FutureTask<ArrayList<OUT>> startListening(int port, OutputVerifier<OUT> verifier) {
		FutureTask<ArrayList<OUT>> future =
				new FutureTask<ArrayList<OUT>>(new OutputListener<OUT>(port,verifier));

		//listen for org.apache.flink.streaming.test.output.org.apache.flink.streaming.test.output
		executorService.execute(future);
		return future;
	}

	/**
	 * Waits for sink to close and returns output
	 * @return output from sink
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public ArrayList<OUT> getTestResult() throws ExecutionException, InterruptedException {
		return outputFuture.get();
	}

}
