package org.apache.flink.streaming.test.tool.output.assertion.result;

import com.google.common.collect.Iterables;
import org.apache.commons.collections.IteratorUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public abstract class UntilRecord<T> extends TypeSafeDiagnosingMatcher<Iterable<T>> {

	private final Matcher<T> matcher;

	public UntilRecord(Matcher<T> matcher) {
		this.matcher = matcher;
	}

	@Override
	public boolean matchesSafely(Iterable<T> objects, Description mismatch) {
		int matches = 0;
		int possibleMatches = Iterables.size(objects);
		Description mismatches = new StringDescription();
		System.out.println("called");
		for (T item : objects) {
			System.out.println("item = " + item.toString());
			if (!matcher.matches(item)) {
//				mismatch.appendDescriptionOf(matcher).appendText(" ");
				matcher.describeMismatch(item, mismatches);
			} else {
				matches++;

				if (validWhen(matches, possibleMatches)) {
					return true;
				}
			}
			System.out.println("matches = " + matches);
		}
		describeMismatch(matches, mismatch, mismatches);
		return false;
	}

	private void describeMismatch(int matches,
								  Description mismatch,
								  Description mismatches) {
		mismatch.appendText("expected matches to be ");
		describeCondition(mismatch);
		mismatch.appendText(" was ")
				.appendValue(matches)
				.appendText(" because:")
				.appendText(mismatches.toString());
	}

	protected abstract Description describeCondition(Description description);

	protected abstract boolean validWhen(int matches, int possibleMatches);

	protected abstract String prefix();

	@Override
	public void describeTo(Description description) {
		description.appendText(prefix()).appendDescriptionOf(matcher);
	}


}


