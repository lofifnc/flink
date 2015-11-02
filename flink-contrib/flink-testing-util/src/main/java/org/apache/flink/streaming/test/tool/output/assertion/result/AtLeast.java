package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher}s that is successful if at least n
 * items in the examined {@list Iterable} is a positive match.
 * @param <T>
 */
public class AtLeast<T> extends UntilList<T> {

	private final int n;

	/**
	 * Default Constructor
	 * @param matcher to apply to the {@link Iterable}
	 * @param n number of expected positive matches
	 */
	public AtLeast(Matcher<T> matcher,int n) {
		super(matcher);
		this.n = n;
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("at least ").appendValue(n);
	}

	@Override
	protected boolean validWhen(int matches, int possibleMatches) {
		return matches == n;
	}

	@Override
	public String prefix() {
		return "at least ";
	}

	@Factory
	public static <T> AtLeast<T> atLeast(Matcher<T> matcher, int n) {
		return new AtLeast<>(matcher, n);
	}

}