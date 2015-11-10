package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.apache.flink.streaming.test.tool.output.assertion.tuple.TupleMapMatchers;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

/**
 * Enables the use of a {@link TupleMask} to map a {@link Tuple} to string keys.
 * And then use these keys in combination with hamcrest {@link Matcher}s.
 *
 * @param <T>
 */
public class AssertBlock<T extends Tuple> {

	private final TupleMask<T> mask;
	private List<KeyMatcherPair> matchers = new ArrayList<>();

	/**
	 * Constructor
	 *
	 * @param fields list of keys
	 */
	public AssertBlock(String... fields) {
		this(new TupleMask<T>(fields));
	}

	/**
	 * Constructor
	 *
	 * @param mask to use
	 */
	public AssertBlock(TupleMask<T> mask) {
		this.mask = mask;
	}

	public static <T extends Tuple> AssertBlock<T> fromMask(TupleMask<T> mask) {
		return new AssertBlock<T>(mask);
	}

	/**
	 * Add a new assertion to the list.
	 *
	 * @param key   of the field
	 * @param match matcher to use on the field
	 * @return
	 */
	public AssertBlock<T> assertThat(String key, Matcher match) {
		matchers.add(KeyMatcherPair.of(key, match));
		return this;
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> anyOfThem() {
		return new ResultMatcher<>(TupleMapMatchers.any(matchers, mask));
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> eachOfThem() {
		return new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> exactlyNOfThem(int n) {
		return new ResultMatcher<>(TupleMapMatchers.exactly(matchers, mask, n));
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> atLeastNOfThem(int n) {
		return new ResultMatcher<>(TupleMapMatchers.atLeast(matchers, mask, n));
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> atMostNOfThem(int n) {
		return new ResultMatcher<>(TupleMapMatchers.atMost(matchers, mask, n));
	}

}
