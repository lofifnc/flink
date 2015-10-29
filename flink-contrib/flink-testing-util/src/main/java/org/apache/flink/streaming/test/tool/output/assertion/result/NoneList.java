package org.apache.flink.streaming.test.tool.output.assertion.result;

import org.hamcrest.Description;
import org.hamcrest.Matcher;

@Deprecated
public class NoneList<T> extends WhileList<T> {

	public NoneList(Matcher<T> matcher) {
		super(matcher);
	}

	@Override
	protected Description describeCondition(Description description) {
		return description.appendValue(0);
	}

	@Override
	public String prefix() {
		return "none of ";
	}

	@Override
	public boolean validWhile(int matches) {
		return matches == 0;
	}
}
