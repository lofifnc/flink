package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.output.assertion.result.QuantifyMatchers;
import org.hamcrest.Matcher;


public class ResultMatcher<T extends Tuple> {

	private final Matcher<T> matcher;

	public ResultMatcher(Matcher<T> matcher) {
		this.matcher = matcher;
	}

	public OutputMatcher<T> any() {
		return OutputMatcher.create(QuantifyMatchers.any(matcher));
	}

	public OutputMatcher<T>  each() {
		return OutputMatcher.create(QuantifyMatchers.each(matcher));
	}

	public OutputMatcher<T>  one() {
		return OutputMatcher.create(QuantifyMatchers.each(matcher));
	}

	public OutputMatcher<T> none() {
		return OutputMatcher.create(QuantifyMatchers.none(matcher));
	}


}