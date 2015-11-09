package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.output.assertion.result.RecordsMatchers;
import org.hamcrest.Matcher;


public class ResultMatcher<T extends Tuple> {

	private final Matcher<T> matcher;

	public ResultMatcher(Matcher<T> matcher) {
		this.matcher = matcher;
	}

	public OutputMatcher<T> onAnyRecord() {
		return OutputMatcher.create(RecordsMatchers.any(matcher));
	}

	public OutputMatcher<T> onEachRecord() {
		return OutputMatcher.create(RecordsMatchers.each(matcher));
	}

	public OutputMatcher<T> onOneRecord() {
		return OutputMatcher.create(RecordsMatchers.each(matcher));
	}

	public OutputMatcher<T> onNoRecord() {
		return OutputMatcher.create(RecordsMatchers.none(matcher));
	}

	public OutputMatcher<T> onExatlyNRecords(int n) {
		return OutputMatcher.create(RecordsMatchers.exactly(matcher, n));
	}

	public OutputMatcher<T> onatLeastNRecords(int n) {
		return OutputMatcher.create(RecordsMatchers.atLeast(matcher,n));
	}

	public OutputMatcher<T> onatMostNRecords(int n) {
		return OutputMatcher.create(RecordsMatchers.atMost(matcher,n));
	}


}