package org.apache.flink.streaming.test.tool.output.assertion;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

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
