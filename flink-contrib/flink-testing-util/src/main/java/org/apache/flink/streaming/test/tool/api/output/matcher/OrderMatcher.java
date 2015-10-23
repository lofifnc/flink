package org.apache.flink.streaming.test.tool.api.output.matcher;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.streaming.test.tool.matcher.partial.FromPartialMatcher;

import java.util.Arrays;
import java.util.List;

public class OrderMatcher<T> {

	private org.apache.flink.streaming.test.tool.matcher.partial.OrderMatcher<T> matcher;

	public OrderMatcher(org.apache.flink.streaming.test.tool.matcher.ListMatcherBuilder<T> builder) {
		matcher = org.apache.flink.streaming.test.tool.matcher.partial.OrderMatcher.createFromBuilder(builder);
	}

	public FromPartialMatcher from(int n) {
		return matcher.from(n);
	}

	public void to(int n) {
		matcher.to(n);
	}

	public void all() {
		matcher.all();
	}

	public void indices(int first, int second, int... rest) {
		int[] front = new int[]{first,second};
		List<Integer> list = Arrays.asList(ArrayUtils.toObject(ArrayUtils.addAll(front, rest)));
		matcher.indices(list);
	}

}
