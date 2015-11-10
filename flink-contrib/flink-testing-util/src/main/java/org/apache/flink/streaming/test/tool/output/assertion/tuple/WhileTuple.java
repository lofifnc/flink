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

package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.TupleMap;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.List;

abstract public class WhileTuple<T extends Tuple> extends TypeSafeDiagnosingMatcher<T> {
	//TODO play with description

	private final Iterable<KeyMatcherPair> matcherKeyPairs;
	private final TupleMask<T> table;

	public WhileTuple(Iterable<KeyMatcherPair> matchers,
					TupleMask<T> table) {
		this.matcherKeyPairs = matchers;
		this.table = table;
	}

	@Override
	public boolean matchesSafely(T tuple, Description mismatch) {

		TupleMap tupleMap = table.convert(tuple);
		int matches = 0;
		for (KeyMatcherPair matcherKeyPair : matcherKeyPairs) {

			Object object = tupleMap.get(matcherKeyPair.key);
			Matcher matcher = matcherKeyPair.matcher;
			if (!matcher.matches(object)) {
				mismatch.appendDescriptionOf(matcher).appendText(" ");
				matcher.describeMismatch(object, mismatch);
			} else {
				matches++;
			// Check exit condition if not valid exit matcher.
				if (!validWhile(matches)) {
					return false;
				}
			}
		}
		return validAfter(matches);
	}

	@Override
	public void describeTo(Description description) {
		List<Matcher> matchers = new ArrayList<>();
		for (KeyMatcherPair m : matcherKeyPairs) {
			matchers.add(m.matcher);
		}
		description.appendText(prefix());
		description.appendList("(", ";" + " ", ")", matchers);
	}

	public abstract boolean validWhile(int matches);

	public abstract String prefix();

	public boolean validAfter(int matches){
		return true;
	}

}
