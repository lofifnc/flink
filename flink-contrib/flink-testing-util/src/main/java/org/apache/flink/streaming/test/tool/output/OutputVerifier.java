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

import org.apache.flink.streaming.test.tool.runtime.messaging.OutputListener;
import org.scalatest.exceptions.TestFailedException;

/**
 * Implement this interface to verify the input
 * from {@link TestSink}.
 * @param <T>
 */
public interface OutputVerifier<T> {

	/**
	 * This method is called when the {@link OutputListener} has
	 * been successfully initialized
	 */
	void init();

	/**
	 * This method is called when a record has arrived
	 * at the test sink.
	 * @param elem record that is received by the test sink
	 * @throws TestFailedException if the output is not valid
	 */
	void receive(T elem) throws TestFailedException;

	/**
	 * This method is called by the {@link OutputListener}
	 * when the test is finished and the last record has
	 * been received
	 * @throws TestFailedException if the complete output is not valid
	 */
	void finish() throws TestFailedException;

}
