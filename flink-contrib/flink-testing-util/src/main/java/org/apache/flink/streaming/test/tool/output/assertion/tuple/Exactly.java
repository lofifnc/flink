package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher} that is successful if exactly n
 * items in the examined {@link Iterable} is a positive match.
 *
 * @param <T>
 */
public class Exactly<T extends Tuple> extends WhileTuple<T> {

	private final int n;

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link KeyMatcherPair}
	 * @param n        number of expected matches
	 */
	public Exactly(Iterable<KeyMatcherPair> matchers, TupleMask<T> mask, int n) {
		super(matchers, mask);
		this.n = n;
	}

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
	public static <T extends Tuple> Exactly<T> exactly(Iterable<KeyMatcherPair> matchers,
													   TupleMask<T> mask,
													   int n) {
		return new Exactly<T>(matchers, mask, n);
	}
}


