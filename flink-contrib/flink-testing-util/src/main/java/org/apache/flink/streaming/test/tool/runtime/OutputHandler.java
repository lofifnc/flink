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

import org.apache.flink.streaming.test.tool.output.OutputVerifier;
import org.apache.flink.streaming.test.tool.runtime.messaging.OutputListener;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * Provides a asynchronous method to wait for test results
 *
 * @param <OUT> type of data the sink is sending to the listener.
 */
public class OutputHandler<OUT> {

	private ExecutorService executorService = Executors.newSingleThreadExecutor();
	private FutureTask<Boolean> outputFuture;
//	private OutputVerifier<OUT> verifier;

	/**
	 * Starts a listener in the background
	 * @param port
	 */
	public OutputHandler(Integer port, OutputVerifier<OUT> verifier) {
		outputFuture = startListening(port,verifier);
	}


	private FutureTask<Boolean> startListening(int port, OutputVerifier<OUT> verifier) {
		FutureTask<Boolean> future =
				new FutureTask<Boolean>(new OutputListener<OUT>(port,verifier));

		//listen for output
		executorService.execute(future);
		return future;
	}

	/**
	 * Stop the test process.
	 * The running verifiers will be finished,
	 * with the current state of the test execution.
	 */
	public void stop() {
		outputFuture.cancel(true);
	}

	/**
	 * Waits for sink to close and returns output
	 * @return output from sink
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public Boolean getTestResult() throws ExecutionException, InterruptedException {
		return outputFuture.get();
	}

}
