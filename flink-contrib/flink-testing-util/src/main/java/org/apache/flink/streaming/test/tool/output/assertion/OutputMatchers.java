package org.apache.flink.streaming.test.tool.output.assertion;

import org.hamcrest.core.AllOf;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.IsNot;

/**
 * Wrapper around default matchers, to combine matchers, from hamcrest.
 */
public class OutputMatchers {

	/**
	 * Creates a matcher that matches if the examined object matches <b>ALL</b> of the specified matchers.
	 * <p/>
	 * For example:
	 * <pre>assertThat("myValue", allOf(startsWith("my"), containsString("Val")))</pre>
	 */
	@SafeVarargs
	public static <T> OutputMatcher<T> allOf(OutputMatcher<T>... matchers) {
		return OutputMatcher.create(AllOf.allOf(matchers));
	}


	/**
	 * Creates a matcher that matches if the examined object matches <b>ANY</b> of the specified matchers.
	 * <p/>
	 * For example:
	 * <pre>assertThat("myValue", anyOf(startsWith("foo"), containsString("Val")))</pre>
	 */
	@SafeVarargs
	public static <T> OutputMatcher anyOf(OutputMatcher<T>... matchers) {
		return OutputMatcher.create(AnyOf.anyOf(matchers));
	}

	/**
	 * Creates a matcher that wraps an existing matcher, but inverts the logic by which
	 * it will match.
	 * <p/>
	 * For example:
	 * <pre>assertThat(cheese, is(not(equalTo(smelly))))</pre>
	 *
	 * @param matcher
	 *     the matcher whose sense should be inverted
	 */
	public static <T> OutputMatcher<T> not(OutputMatcher<T> matcher) {
		return OutputMatcher.create(IsNot.not(matcher));
	}

	/**
	 * Creates a matcher that matches when both of the specified matchers match the examined object.
	 * <p/>
	 * For example:
	 * <pre>assertThat("fab", both(containsString("a")).and(containsString("b")))</pre>
	 */
	public static <LHS> org.hamcrest.core.CombinableMatcher.CombinableBothMatcher<Iterable<LHS>> both(OutputMatcher<LHS> matcher) {
		return org.hamcrest.core.CombinableMatcher.both(matcher);
	}

	/**
	 * Creates a matcher that matches when either of the specified matchers match the examined object.
	 * <p/>
	 * For example:
	 * <pre>assertThat("fan", either(containsString("a")).and(containsString("b")))</pre>
	 */
	public static <LHS> org.hamcrest.core.CombinableMatcher.CombinableEitherMatcher<Iterable<LHS>> either(OutputMatcher<LHS> matcher) {
		return org.hamcrest.core.CombinableMatcher.either(matcher);
	}

}
