package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.hamcrest.Matcher;
import org.hamcrest.core.IsNot;

/**
 * Offers a set of factory methods to create
 * {@link Matcher}s that takes an {@link Iterable}
 * and excepts a certain number of positive matches.
 */
public class RecordsMatchers {

	/**
	 * Creates a {@link Matcher} that is successful if at least one
	 * item in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> any(Matcher<T> matcher) {
		return AnyOf.<T>any(matcher);
	}

	/**
	 * Creates a {@link Matcher} that is successful if each
	 * item in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> each(Matcher<T> matcher) {
		return EachOf.<T>each(matcher);
	}

	/**
	 * Creates a {@link Matcher} that is successful if exactly one
	 * item in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static  <T> Matcher<Iterable<T>> one(Matcher<T> matcher) {
		return OneOf.<T>one(matcher);
	}

	/**
	 * Creates a {@link Matcher} that is successful if at least a number of
	 * items in the examined {@link Iterable} is a positive  match.
	 * @param matcher to apply to the list
	 * @param n number of positive matches
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> atLeast(Matcher<T> matcher, int n) {
		return AtLeast.<T>atLeast(matcher,n);
	}

	/**
	 * Creates a {@link Matcher} that is successful if at most a number of
	 * items in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param n number of positive matches
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> atMost(Matcher<T> matcher, int n) {
		return AtMost.<T>atMost(matcher,n);
	}

	/**
	 * Creates a {@link Matcher} that is successful if an exact number of
	 * items in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param n number of positive matches
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> exactly(Matcher<T> matcher, int n) {
		return Exactly.<T>exactly(matcher,n);
	}

	/**
	 * Creates a {@link Matcher} that is successful if an exact number of
	 * items in the examined {@link Iterable} is a positive match.
	 * @param matcher to apply to the list
	 * @param <T>
	 * @return {@link Matcher}
	 */
	public static <T> Matcher<Iterable<T>> none(Matcher<T> matcher) {
		return IsNot.not(any(matcher));
	}



}
