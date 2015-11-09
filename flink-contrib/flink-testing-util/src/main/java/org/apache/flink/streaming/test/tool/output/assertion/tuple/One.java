package org.apache.flink.streaming.test.tool.output.assertion.tuple;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.output.map.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

/**
 * Provides a {@link Matcher} that is successful if exactly one
 * item in the examined {@link Iterable} is a positive match.
 *
 * @param <T>
 */
public class One<T extends Tuple> extends WhileTuple<T> {

	/**
	 * Default constructor
	 *
	 * @param matchers {@link Iterable} of {@link KeyMatcherPair}
	 */
	public One(Iterable<KeyMatcherPair> matchers, TupleMask<T> mask) {
		super(matchers, mask);
	}

	protected Description describeCondition(Description description) {
		return description.appendText("exactly ").appendValue(1);
	}

	@Override
	public String prefix() {
		return "one of ";
	}

	@Override
	public boolean validWhile(int matches) {
		return matches <= 1;
	}

	@Override
	public boolean validAfter(int matches) {
		return matches == 1;
	}

	@Factory
	public static <T extends Tuple> One<T> one(Iterable<KeyMatcherPair> matchers,
											   TupleMask<T> mask) {
		return new One<T>(matchers, mask);
	}
}
