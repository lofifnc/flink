package org.apache.flink.streaming.test.tool.core;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple;

public class TupleMap<T extends Tuple>{

	private final T tuple;
	private String[] keys;

	public TupleMap(T tuple,String... names) {
		this.tuple = tuple;
		keys = names;
	}

	public <IN> IN get(String key) {
		int index = ArrayUtils.indexOf(keys, key);
		return tuple.getField(index);
	}

	public String[] getKeys() {
		return keys;
	}

	public String toString() {
		return tuple.toString();
	}

}
