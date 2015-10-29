package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.test.tool.core.output.map.OutputTable;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;

public class Each<T extends Tuple> extends Until<T> {

	public Each(Iterable<Tuple2<String, Matcher>> matchers, OutputTable<T> table) {
		super(matchers, table);
	}

	@Override
	public String prefix() {
		return "each of";
	}

	@Override
	public boolean validWhen(int matches, int possibleMatches) {
		return matches == possibleMatches;
	}

	@Factory
	public static <T extends Tuple> Each<T> each(Iterable<Tuple2<String, Matcher>> matchers,
												 OutputTable<T> table) {
		return new Each<T>(matchers,table);
	}
}
