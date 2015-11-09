package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.hamcrest.Description;
import org.hamcrest.Matcher;


/**
 * Provides a {@link Matcher} that is successful if each of
 * the items in the examined {@link Iterable} is a positive match.
 * @param <T>
 */
public class EachOf<T> extends UntilRecord<T> {

	/**
	 * Default Constructor
	 * @param matcher to apply to {@link Iterable}.
	 */
	public EachOf(Matcher<T> matcher) {
		super(matcher);
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendText("<all>");
	}

	@Override
	public boolean validWhen(int matches, int possibleMatches) {
		return matches == possibleMatches;
	}

	@Override
	public String prefix() {
		return "each of ";
	}

	public static <T> EachOf<T> each(Matcher<T> matcher) {
		return new EachOf<>(matcher);
	}
}
