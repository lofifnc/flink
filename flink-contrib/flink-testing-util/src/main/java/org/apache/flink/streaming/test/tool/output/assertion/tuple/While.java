package org.apache.flink.streaming.test.tool.output.assertion.tuple;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.test.tool.TupleMap;
import org.apache.flink.streaming.test.tool.core.output.map.OutputTable;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.ArrayList;
import java.util.List;

abstract public class While<T extends Tuple> extends TypeSafeDiagnosingMatcher<T> {
	//TODO play with description

	private final Iterable<Tuple2<String,Matcher>> matcherKeyPairs;
	private final OutputTable<T> table;

	public While(Iterable<Tuple2<String,Matcher>> matchers,
				 OutputTable<T> table) {
		this.matcherKeyPairs = matchers;
		this.table = table;
	}

	@Override
	public boolean matchesSafely(T tuple, Description mismatch) {

		TupleMap tupleMap = table.convert(tuple);
		int matches = 0;
		for (Tuple2<String,Matcher> matcherKeyPair : matcherKeyPairs) {

			Object object = tupleMap.get(matcherKeyPair.f0);
			Matcher matcher = matcherKeyPair.f1;
			if (!matcher.matches(object)) {
				mismatch.appendDescriptionOf(matcher).appendText(" ");
				matcher.describeMismatch(object, mismatch);
			} else {
				matches++;
			// Check exit condition if not valid exit matcher.
				if (!validWhile(matches)) {
					return false;
				}
			}
		}
		return validAfter(matches);
	}

	@Override
	public void describeTo(Description description) {
		List<Matcher> matchers = new ArrayList<>();
		for (Tuple2<String, Matcher> m : matcherKeyPairs) {
			matchers.add(m.f1);
		}
		description.appendText(prefix());
		description.appendList("(", ";" + " ", ")", matchers);
	}

	public abstract boolean validWhile(int matches);

	public abstract String prefix();

	public boolean validAfter(int matches){
		return true;
	}

}
