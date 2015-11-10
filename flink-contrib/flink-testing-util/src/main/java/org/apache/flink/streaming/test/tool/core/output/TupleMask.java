package org.apache.flink.streaming.test.tool.core.output;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.TupleMap;

public class TupleMask<T extends Tuple> {
	private String[] keys;

	public TupleMask(String... cols) {
		keys = cols;
	}

	public TupleMap<T> convert(T tuple) {
		return new TupleMap<T>(tuple,keys);
	}

	public static <T extends Tuple> TupleMask<T> create(String... cols){
		return new TupleMask<>(cols);
	}

}
