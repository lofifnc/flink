package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.hamcrest.Matcher;

/**
 * Offers a set of factory methods to create {{@link Matcher}s,
 * taking an {@link Iterable} with Pairs of Keys and {@link Matcher}s
 * and ensure a certain number of positive matches.
 */
public class TupleMapMatchers {

	/**
	 * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
	 * fulfill at least one of the specified matchers.
	 *
	 * @param matchers key matcher pairs
	 * @param mask     used for mapping the keys
	 * @param <T>
	 * @return
	 */
	public static <T extends Tuple> Matcher<T> any(Iterable<KeyMatcherPair> matchers,
												   TupleMask<T> mask) {
		return Any.any(matchers, mask);
	}

	/**
	 * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
	 * fulfill all of the specified matchers.
	 *
	 * @param matchers key matcher pairs
	 * @param mask     used for mapping the keys
	 * @param <T>
	 * @return
	 */
	public static <T extends Tuple> Matcher<T> each(Iterable<KeyMatcherPair> matchers,
													TupleMask<T> mask) {
		return Each.each(matchers, mask);
	}

	/**
	 * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
	 * fulfill exactly one of the specified matchers.
	 *
	 * @param matchers key matcher pairs
	 * @param mask     used for mapping the keys
	 * @param <T>
	 * @return
	 */
	public static <T extends Tuple> Matcher<T> one(Iterable<KeyMatcherPair> matchers,
												   TupleMask<T> mask) {
		return One.one(matchers, mask);
	}

	/**
	 * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
	 * fulfill an exact number of the specified matchers.
	 *
	 * @param matchers key matcher pairs
	 * @param mask     used for mapping the keys
	 * @param n        number of matches
	 * @param <T>
	 * @return
	 */
	public static <T extends Tuple> Matcher<T> exactly(Iterable<KeyMatcherPair> matchers,
													   TupleMask<T> mask,
													   int n) {
		return Exactly.exactly(matchers, mask, n);
	}

	/**
	 * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
	 * fulfill at least a number of the specified matchers.
	 *
	 * @param matchers key matcher pairs
	 * @param mask     used for mapping the keys
	 * @param n        number of matches
	 * @param <T>
	 * @return
	 */
	public static <T extends Tuple> Matcher<T> atLeast(Iterable<KeyMatcherPair> matchers,
													   TupleMask<T> mask,
													   int n) {
		return AtLeast.atLeast(matchers, mask, n);
	}

	/**
	 * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
	 * fulfill at most a number of the specified matchers.
	 *
	 * @param matchers key matcher pairs
	 * @param mask     used for mapping the keys
	 * @param n        number of matches
	 * @param <T>
	 * @return
	 */
	public static <T extends Tuple> Matcher<T> atMost(Iterable<KeyMatcherPair> matchers,
													  TupleMask<T> mask,
													  int n) {
		return AtMost.atMost(matchers, mask, n);
	}

}
