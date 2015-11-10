package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.TupleMap;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.List;

public abstract class UntilTuple<T extends Tuple> extends TypeSafeDiagnosingMatcher<T> {

	private final Iterable<KeyMatcherPair> keyMatcherPairs;
	private final TupleMask<T> table;

	public UntilTuple(Iterable<KeyMatcherPair> keyMatcherPairs,
					  TupleMask<T> table) {
		this.table = table;
		this.keyMatcherPairs = keyMatcherPairs;
	}

	@Override
	public boolean matchesSafely(T tuple, Description mismatch) {
		int matches = 0;
		int possibleMatches = Iterables.size(keyMatcherPairs);
		TupleMap<T> tupleMap = table.convert(tuple);

		for (KeyMatcherPair keyMatcherPair : keyMatcherPairs) {
			String key = keyMatcherPair.key;
			Object object = tupleMap.get(key);
			Matcher matcher = keyMatcherPair.matcher;
			if (!matcher.matches(object)) {
				if(!mismatch.toString().endsWith("but: ")) {
					mismatch.appendText("\n          ");
				}
				mismatch
						.appendText("[" + key + "] ")
						.appendDescriptionOf(matcher)
						.appendText(", ");
				matcher.describeMismatch(object, mismatch);
			} else {
				matches++;
				if (validWhen(matches,possibleMatches)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public void describeTo(Description description) {
		List<Matcher> matchers = new ArrayList<>();
		description.appendText("( ");
		for (KeyMatcherPair m : keyMatcherPairs) {
			if(!description.toString().endsWith("( ")) {
				description.appendText("; ");
			}
			description.appendText("[" + m.key + "] ");
			description.appendDescriptionOf(m.matcher);
		}
		description.appendText(") ");
	}

	public abstract String prefix();

	public abstract boolean validWhen(int matches, int possibleMatches);
}
