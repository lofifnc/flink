package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.apache.flink.streaming.test.tool.output.assertion.result.WhileList;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher} that is successful if exactly one
 * item in the examined {@list Iterable} is a positive match.
 * @param <T>
 */
public class OneOf<T> extends WhileList<T> {

	/**
	 * Default constructor
	 * @param matcher to apply to the {@link Iterable}
	 */
	public OneOf(Matcher<T> matcher) {
		super(matcher);
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("exactly ").appendValue(1);
	}

	@Override
	public String prefix() {
		return "one of ";
	}

	@Override
	public boolean validWhile(int matches) {
		return matches <= 1;
	}

	@Override
	public boolean validAfter(int matches) {
		return matches == 1;
	}

	@Factory
	public static <T> OneOf<T> one(Matcher<T> matcher) {
		return new OneOf<T>(matcher);
	}
}
