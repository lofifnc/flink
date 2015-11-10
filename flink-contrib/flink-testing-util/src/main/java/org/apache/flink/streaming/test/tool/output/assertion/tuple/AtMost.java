package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Factory;

/**
 * Provides a {@link org.hamcrest.Matcher} inspecting a {@link Tuple} and expecting it to
 * fulfill at most one of the specified matchers.
 */
public class AtMost<T extends Tuple> extends WhileTuple<T> {

	private final int n;

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link KeyMatcherPair}
	 * @param n        number of expected matches
	 */
	public AtMost(Iterable<KeyMatcherPair> matchers, TupleMask<T> mask, int n) {
		super(matchers, mask);
		this.n = n;
	}

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
	public static <T extends Tuple> AtMost<T> atMost(Iterable<KeyMatcherPair> matchers,
													 TupleMask<T> mask,
													 int n) {
		return new AtMost<T>(matchers, mask, n);
	}
}
