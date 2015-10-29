package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.test.tool.core.output.map.OutputTable;
import org.hamcrest.Matcher;

/**
 * Offers a set of factory methods to create {{@link Matcher}s,
 * taking an {@link Iterable} with Pairs of Keys and {@link Matcher}s
 * and ensure a certain number of positive matches.
 */
public class TupleMapMatchers {

	/**
	 * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
	 * fulfill at least one of the specified matchers.
	 * @param matchers key matcher pairs
	 * @param table used for mapping the keys
	 * @param <T>
	 * @return
	 */
	public static <T extends Tuple> Matcher<T> any(Iterable<Tuple2<String,Matcher>> matchers,
																OutputTable<T> table) {
		return Any.any(matchers,table);
	}

	/**
	 * Creates a {@link Matcher} inspecting a {@link Tuple} and expecting it to
	 * fulfill all of the specified matchers.
	 * @param matchers key matcher pairs
	 * @param table used for mapping the keys
	 * @param <T>
	 * @return
	 */
	public static <T extends Tuple> Matcher<T> each(Iterable<Tuple2<String,Matcher>> matchers,
											 OutputTable<T> table) {
		return Each.each(matchers,table);
	}

}
