package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;


/**
 * Provides a {@link Matcher} that is successful if exactly n
 * items in the examined {@link Iterable} is a positive match.
 *
 * @param <T>
 */
public class Exactly<T> extends WhileRecord<T> {

	private final int n;

	/**
	 * Default constructor
	 *
	 * @param matcher to apply to the {@link Iterable}
	 * @param n number of expected positive matches
	 */
	public Exactly(Matcher<T> matcher, int n) {
		super(matcher);
		this.n = n;
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("exactly ").appendValue(n);
	}

	@Override
	public String prefix() {
		return "exactly ";
	}

	@Override
	public boolean validWhile(int matches) {
		return matches <= n;
	}

	@Override
	public boolean validAfter(int matches) {
		return matches == n;
	}

	@Factory
	public static <T> Exactly<T> exactly(Matcher<T> matcher, int n) {
		return new Exactly<T>(matcher,n);
	}
}

