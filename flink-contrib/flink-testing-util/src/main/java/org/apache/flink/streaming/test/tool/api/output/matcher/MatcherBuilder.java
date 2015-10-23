package org.apache.flink.streaming.test.tool.api.output.matcher;

import java.util.List;

/**
 * Wrapper for the scala {@link org.apache.flink.streaming.test.tool.matcher.ListMatcherBuilder}
 * @param <T>
 */
public class MatcherBuilder<T> {

	private org.apache.flink.streaming.test.tool.matcher.ListMatcherBuilder<T> builder;
	private List<T> right;

	public MatcherBuilder(List<T> right) {
		this.right = right;
		builder = new org.apache.flink.streaming.test.tool.matcher.ListMatcherBuilder<>(right);
	}

	/**
	 * Tests whether the output contains only the expected records
	 */
	public MatcherBuilder<T> only() {
		builder.only();
		return this;
	}

	/**
	 * Tests whether the output contains no duplicates in reference
	 * to the expected output
	 */
	public MatcherBuilder<T> noDuplicates() {
		builder.noDuplicates();
		return this;
	}

	/**
	 * Provides a {@link OrderMatcher} to verify the order of
	 * elements in the output
	 */
	public OrderMatcher<T> inOrder() {
		return new OrderMatcher<T>(builder);
	}

	/**
	 * Tests whether the list matches the expectations
	 * @param left actual output
	 */
	public void verify(List<T> left) {
		builder.verify(left);
	}

}
