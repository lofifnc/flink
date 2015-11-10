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

package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.hamcrest.Description;
import org.hamcrest.Matcher;


/**
 * Provides a {@link Matcher} that is successful if each of
 * the items in the examined {@link Iterable} is a positive match.
 * @param <T>
 */
public class EachOf<T> extends UntilRecord<T> {

	/**
	 * Default Constructor
	 * @param matcher to apply to {@link Iterable}.
	 */
	public EachOf(Matcher<T> matcher) {
		super(matcher);
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("<all>");
	}

	@Override
	public boolean validWhen(int matches, int possibleMatches) {
		return matches == possibleMatches;
	}

	@Override
	public String prefix() {
		return "each of ";
	}

	public static <T> EachOf<T> each(Matcher<T> matcher) {
		return new EachOf<>(matcher);
	}
}
