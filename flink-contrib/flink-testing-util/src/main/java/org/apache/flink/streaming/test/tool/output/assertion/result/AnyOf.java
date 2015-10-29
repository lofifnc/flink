package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher}s that is successful if at least one
 * item in the examined {@list Iterable} is a positive match.
 * @param <T>
 */
public class AnyOf<T> extends UntilList<T> {

	/**
	 * Default Constructor
	 * @param matcher to apply to the {@link Iterable}
	 */
	public AnyOf(Matcher<T> matcher) {
		super(matcher);
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("at least ").appendValue(1);
	}

	@Override
	protected boolean validWhen(int matches, int possibleMatches) {
		return matches == 1;
	}

	@Override
	public String prefix() {
		return "any of ";
	}

	@Factory
	public static <T> AnyOf<T> any(Matcher<T> matcher) {
		return new AnyOf<>(matcher);
	}

}
