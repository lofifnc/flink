package api.input;

import input.EventTimeInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

public class EventTimeInputBuilder<T> implements EventTimeInput<T> {

	private ArrayList<StreamRecord<T>> input = new ArrayList<>();

	private EventTimeInputBuilder(StreamRecord<T> record) {
		input.add(record);
	}

	public static <T> EventTimeInputBuilder<T> addFirst(T elem, long timeStamp) {
		return new EventTimeInputBuilder<T>(new StreamRecord<T>(elem,timeStamp));
	}

	public static <T> EventTimeInputBuilder<T> addFirst(StreamRecord<T> record) {
		return new EventTimeInputBuilder<T>(record);
	}

	public static <T> EventTimeInputBuilder<T> addFirst(T elem) {
		return new EventTimeInputBuilder<T>(new StreamRecord<T>(elem, System.currentTimeMillis()));
	}

	public EventTimeInputBuilder<T> add(T elem, long timeStamp) {
		input.add(new StreamRecord<T>(elem, timeStamp));
		return this;
	}

	public EventTimeInputBuilder<T> add(T elem, After after) {
		long lastTimeStamp = input.get(input.size() - 1).getTimestamp();
		long currentTimeStamp = lastTimeStamp + after.getPeriod();
		input.add(new StreamRecord<T>(elem, currentTimeStamp));
		return this;
	}

	public EventTimeInputBuilder<T> add(StreamRecord<T> record) {
		input.add(record);
		return this;
	}

	@Override
	public List<StreamRecord<T>> getInput() {
		return input;
	}
}
