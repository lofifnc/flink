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

package org.apache.flink.streaming.test.tool.core.output;

import org.apache.flink.streaming.test.tool.core.output.matcher.MatcherBuilder;
import org.apache.flink.streaming.test.tool.output.ListVerifier;
import org.apache.flink.streaming.test.tool.output.OutputVerifier;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to define expectations
 * for the input of a {@link org.apache.flink.streaming.test.tool.output.TestSink}
 * @param <T>
 */
public class ExpectedOutput<T> {

	/** list of expected records */
	private List<T> expectedOutput;
	/** matcher to test the output */
	private MatcherBuilder<T> matcher;

	public ExpectedOutput() {
		expectedOutput = new ArrayList<>();
	}

	/**
	 * Provides the {@link MatcherBuilder} to define
	 * expectations for the test
	 * @return builder for refining expectations
	 */
	public MatcherBuilder<T> expect() {
		if (matcher == null)
			matcher = new MatcherBuilder<>(expectedOutput);
		return matcher;
	}

	/**
	 * Adds an elements to the list of expected output[
	 * @param elem record to add
	 * @return
	 */
	public ExpectedOutput<T> add(T elem) {
		expectedOutput.add(elem);
		return this;
	}

	/**
	 * Returns a {@link OutputVerifier}
	 * that is used by the {@link org.apache.flink.streaming.test.tool.output.OutputHandler}
	 * to test the output.
	 * @return verifier
	 */
	public OutputVerifier<T> getVerifier() {
		/* if no expectations have been defined you will be provided with the default */
		if(matcher == null) {
			expect();
		}
		return new ListVerifier<>(matcher);
	}

//	public <T> getValidator() {
//		return matcher;
//	}

}
