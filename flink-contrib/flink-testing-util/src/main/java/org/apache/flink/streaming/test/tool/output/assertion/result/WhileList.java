package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.apache.flink.streaming.test.tool.TupleMap;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public abstract class WhileList<T> extends TypeSafeDiagnosingMatcher<Iterable<T>> {

	private final Matcher<T> matcher;

	public WhileList(Matcher<T> matcher) {
		this.matcher = matcher;
	}

	@Override
	public boolean matchesSafely(Iterable<T> objects, Description mismatch) {
		int matches = 0;
		Description mismatches = new StringDescription();

		for (T item : objects) {
			if (!matcher.matches(item)) {
				matcher.describeMismatch(item, mismatches);
			} else {
				matches++;
				if (!validWhile(matches)) {
					describeMismatch(matches, true, mismatch, mismatches);
					return false;
				}
			}
		}
		describeMismatch(matches, false, mismatch, mismatches);
		return validAfter(matches);
	}

	@Override
	public void describeTo(Description description) {
		description.appendText(prefix());
		description.appendDescriptionOf(matcher);
	}

	private void describeMismatch(int matches,
								  Boolean tooMany,
								  Description mismatch,
								  Description mismatches) {
		mismatch.appendText("expected matches to be ");
		describeCondition(mismatch);
		mismatch.appendText(" was ")
				.appendValue(matches);
		if(tooMany) {
			mismatch.appendText(" because all records matched, except:");
		}else {
			mismatch.appendText(" because: ");
		}

		mismatch.appendText(mismatches.toString());
	}


	protected abstract Description describeCondition(Description description);

	public abstract String prefix();

	public abstract boolean validWhile(int matches);

	public boolean validAfter(int matches) {
		return true;
	}

}

