package org.apache.flink.streaming.test.tool.core.output.map;


import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.TupleMap;

import java.util.ArrayList;
import java.util.List;

public class OutputTable<T extends Tuple> {
	private List<TupleMap<T>> rows;
	private String[] keys;

//	public static <T extends Tuple> OutputTable<T> create(IN tuple, String... cols) {
//		OutputTable<IN> table = new OutputTable<>(cols);
//		table.add(tuple);
//		return table;
//	}

	public OutputTable(String... cols) {
		rows = new ArrayList<>();
		keys = cols;
	}

	public TupleMap<T> convert(T tuple) {
		return new TupleMap<T>(tuple,keys);
	}

//	public int size() {
//		return rows.size();
//	}

//	public void add(T tuple) {
//		rows.add(new TupleMap<T>(tuple, keys));
//	}

//	@Override
//	public Iterator<TupleMap<T>> iterator() {
//		return rows.iterator();
//	}

//	public TupleMap getRow(int i) {
//		return rows.get(i);
//	}

//	public Iterable<TupleMap<T>> getRows() {
//		return rows;
//	}

//	public void addAll(Iterable<T> output) {
//		for (T tuple : output) {
//			rows.add(new TupleMap<T>(tuple, keys));
//		}
//	}
}
