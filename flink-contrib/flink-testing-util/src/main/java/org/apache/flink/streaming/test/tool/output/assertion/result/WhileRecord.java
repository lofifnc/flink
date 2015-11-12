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
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public abstract class WhileRecord<T> extends TypeSafeDiagnosingMatcher<Iterable<T>> {

	private final Matcher<T> matcher;

	public WhileRecord(Matcher<T> matcher) {
		this.matcher = matcher;
	}

	@Override
	public boolean matchesSafely(Iterable<T> objects, Description mismatch) {
		int matches = 0;
		Description mismatches = new StringDescription();
		int i = 0;
		for (T item : objects) {
			if (!matcher.matches(item)) {
				matcher.describeMismatch(item, mismatches);
				mismatches.appendText(" on record #"+i);
			} else {
				matches++;
				if (!validWhile(matches)) {
					describeMismatch(matches, true, mismatch, mismatches);
					return false;
				}
			}
			i++;
		}
		describeMismatch(matches, false, mismatch, mismatches);
		return validAfter(matches);
	}

	@Override
	public void describeTo(Description description) {
		description.appendText(prefix());
		description.appendDescriptionOf(matcher);
	}

	private void describeMismatch(int matches,
								Boolean tooMany,
								Description mismatch,
								Description mismatches) {
		mismatch.appendText("expected matches to be ");
		describeCondition(mismatch);
		mismatch.appendText(" was ")
				.appendValue(matches);
		if (tooMany) {
			mismatch.appendText(" because all records matched, except:");
		} else {
			mismatch.appendText(" because: ");
		}

		mismatch.appendText(mismatches.toString());
	}


	protected abstract Description describeCondition(Description description);

	public abstract String prefix();

	public abstract boolean validWhile(int matches);

	public boolean validAfter(int matches) {
		return true;
	}

}

