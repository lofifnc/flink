package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Factory;

/**
 * Provides a {@link org.hamcrest.Matcher} inspecting a {@link Tuple} and expecting it to
 * fulfill at least a number of the specified matchers.
 */
public class AtLeast<T extends Tuple> extends UntilTuple<T> {

	private final int n;

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link KeyMatcherPair}
	 * @param n        number of expected matches
	 */
	public AtLeast(Iterable<KeyMatcherPair> matchers, TupleMask<T> mask, int n) {
		super(matchers, mask);
		this.n = n;
	}

	public Description describeCondition(Description description) {
		return description.appendText("at least ").appendValue(n);
	}

	@Override
	public boolean validWhen(int matches, int possibleMatches) {
		return matches == n;
	}

	@Override
	public String prefix() {
		return "at least ";
	}

	@Factory
	public static <T extends Tuple> AtLeast<T> atLeast(Iterable<KeyMatcherPair> matchers,
													   TupleMask<T> mask,
													   int n) {
		return new AtLeast<>(matchers, mask, n);
	}
}
