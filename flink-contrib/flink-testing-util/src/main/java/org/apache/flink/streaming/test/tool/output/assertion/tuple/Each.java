package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.hamcrest.Factory;

/**
 * Provides a {@link org.hamcrest.Matcher} inspecting a {@link Tuple} and expecting it to
 * fulfill at each of the specified matchers.
 */
public class Each<T extends Tuple> extends UntilTuple<T> {

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link KeyMatcherPair}
	 */
	public Each(Iterable<KeyMatcherPair> matchers, TupleMask<T> mask) {
		super(matchers, mask);
	}

	@Override
	public String prefix() {
		return "each of";
	}

	@Override
	public boolean validWhen(int matches, int possibleMatches) {
		return matches == possibleMatches;
	}

	@Factory
	public static <T extends Tuple> Each<T> each(Iterable<KeyMatcherPair> matchers,
												 TupleMask<T> mask) {
		return new Each<T>(matchers, mask);
	}
}
