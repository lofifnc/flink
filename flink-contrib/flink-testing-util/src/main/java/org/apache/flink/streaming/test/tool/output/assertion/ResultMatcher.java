/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.output.assertion.result.RecordsMatchers;
import org.hamcrest.Matcher;


public class ResultMatcher<T extends Tuple> {

	private final Matcher<T> matcher;

	public ResultMatcher(Matcher<T> matcher) {
		this.matcher = matcher;
	}

	public OutputMatcher<T> onAnyRecord() {
		return OutputMatcher.create(RecordsMatchers.any(matcher));
	}

	public OutputMatcher<T> onEachRecord() {
		return OutputMatcher.create(RecordsMatchers.each(matcher));
	}

	public OutputMatcher<T> onOneRecord() {
		return OutputMatcher.create(RecordsMatchers.each(matcher));
	}

	public OutputMatcher<T> onNoRecord() {
		return OutputMatcher.create(RecordsMatchers.none(matcher));
	}

	public OutputMatcher<T> onExatlyNRecords(int n) {
		return OutputMatcher.create(RecordsMatchers.exactly(matcher, n));
	}

	public OutputMatcher<T> onatLeastNRecords(int n) {
		return OutputMatcher.create(RecordsMatchers.atLeast(matcher,n));
	}

	public OutputMatcher<T> onatMostNRecords(int n) {
		return OutputMatcher.create(RecordsMatchers.atMost(matcher,n));
	}


}