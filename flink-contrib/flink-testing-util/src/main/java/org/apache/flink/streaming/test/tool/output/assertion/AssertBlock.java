package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.test.tool.core.output.map.OutputTable;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

public class AssertBlock<T extends Tuple> {

	private final OutputTable<T> table;

	public AssertBlock(String... fields) {
		this.table = new OutputTable<T>(fields);
	}

	private List<Tuple2<String, Matcher>> matchers = new ArrayList<>();

	public AssertBlock<T> assertThat(String key, Matcher match) {
		matchers.add(Tuple2.of(key, match));
		return this;
	}

	public ResultMatcher<T> any() {
		return new ResultMatcher<>(TupleMapMatchers.any(matchers,table));
	}

	public ResultMatcher<T> each() {
		return new ResultMatcher<>(TupleMapMatchers.each(matchers, table));
	}

}
