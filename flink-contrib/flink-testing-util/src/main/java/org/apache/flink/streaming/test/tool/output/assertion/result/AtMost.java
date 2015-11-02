package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher} that is successful if at most n
 * items in the examined {@list Iterable} is a positive match.
 * @param <T>
 */
public class AtMost<T> extends WhileList<T> {

	private final int n;

	/**
	 * Default constructor
	 * @param matcher to apply to the {@link Iterable}
	 *                @param n number of expected matches
	 */
	public AtMost(Matcher<T> matcher, int n) {
		super(matcher);
		this.n = n;
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("at Most ").appendValue(n);
	}

	@Override
	public String prefix() {
		return "at most ";
	}

	@Override
	public boolean validWhile(int matches) {
		return matches <= n;
	}

	@Factory
	public static <T> AtMost<T> atMost(Matcher<T> matcher, int n) {
		return new AtMost<T>(matcher,n);
	}
}