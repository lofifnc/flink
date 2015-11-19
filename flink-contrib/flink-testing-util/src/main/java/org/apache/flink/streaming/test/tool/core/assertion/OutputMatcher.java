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

package org.apache.flink.streaming.test.tool.core.assertion;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

/**
 *
 * @param <T>
 */
abstract public class OutputMatcher<T> extends BaseMatcher<Iterable<T>> {
	public static <T> OutputMatcher<T> create(final Matcher<Iterable<T>> matcher) {
		return new OutputMatcher<T>() {
			@Override
			public boolean matches(Object item) {
				return matcher.matches(item);
			}

			@Override
			public void describeMismatch(Object item, Description mismatchDescription) {
				matcher.describeMismatch(item,mismatchDescription);
			}

			@Override
			public void describeTo(Description description) {
				matcher.describeTo(description);
			}
		};
	}
}
