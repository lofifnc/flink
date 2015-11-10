package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.hamcrest.Factory;

/**
 * Provides a {@link org.hamcrest.Matcher} inspecting a {@link Tuple} and expecting it to
 * fulfill at least one of the specified matchers.
 */
public class Any<T extends Tuple> extends UntilTuple<T> {

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link KeyMatcherPair}
	 */
	public Any(Iterable<KeyMatcherPair> matchers,
			   TupleMask<T> table) {
		super(matchers, table);
	}

	@Override
	public String prefix() {
		return "any of";
	}

	@Override
	public boolean validWhen(int matches, int possibleMatches) {
		return matches == 1;
	}

	@Factory
	public static <T extends Tuple> Any<T> any(Iterable<KeyMatcherPair> matchers,
											   TupleMask<T> table) {
		return new Any<T>(matchers, table);
	}
}
