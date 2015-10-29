package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.test.tool.core.output.map.OutputTable;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

public class Any<T extends Tuple> extends Until<T> {

	public Any(Iterable<Tuple2<String,Matcher>> matchers,
			   OutputTable<T> table) {
		super(matchers,table);
	}

	@Override
	public String prefix() {
		return "any of";
	}

	@Override
	public boolean validWhen(int matches, int possibleMatches) {
		return matches == 1;
	}

	@Factory
	public static <T extends Tuple> Any<T> any(Iterable<Tuple2<String,Matcher>> matchers,
											OutputTable<T> table) {
		return new Any<T>(matchers,table);
	}
}
