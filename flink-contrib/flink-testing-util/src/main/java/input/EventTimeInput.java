package input;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

public interface EventTimeInput<T> {
	List<StreamRecord<T>> getInput();
}
