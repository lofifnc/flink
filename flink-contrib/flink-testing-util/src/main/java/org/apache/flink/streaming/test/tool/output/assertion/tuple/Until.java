package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.test.tool.TupleMap;
import org.apache.flink.streaming.test.tool.core.output.map.OutputTable;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.List;

public abstract class Until<T extends Tuple> extends TypeSafeDiagnosingMatcher<T> {

	private final Iterable<Tuple2<String, Matcher>> matcherKeyPairs;
	private final OutputTable<T> table;

	public Until(Iterable<Tuple2<String, Matcher>> matcherKeyPairs,
				 OutputTable<T> table) {
		this.table = table;
		this.matcherKeyPairs = matcherKeyPairs;
	}

	@Override
	public boolean matchesSafely(T tuple, Description mismatch) {
		int matches = 0;
		int possibleMatches = Iterables.size(matcherKeyPairs);
		TupleMap<T> tupleMap = table.convert(tuple);

		for (Tuple2<String, Matcher> matcherKeyPair : matcherKeyPairs) {
			String key = matcherKeyPair.f0;
			Object object = tupleMap.get(key);
			Matcher matcher = matcherKeyPair.f1;
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
		for (Tuple2<String, Matcher> m : matcherKeyPairs) {
			if(!description.toString().endsWith("( ")) {
				description.appendText("; ");
			}
			description.appendText("[" + m.f0 + "] ");
			description.appendDescriptionOf(m.f1);
		}
		description.appendText(") ");
	}

	public abstract String prefix();

	public abstract boolean validWhen(int matches, int possibleMatches);
}
