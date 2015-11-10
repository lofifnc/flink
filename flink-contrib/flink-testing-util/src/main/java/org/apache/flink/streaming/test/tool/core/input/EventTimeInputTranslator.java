package org.apache.flink.streaming.test.tool.core.input;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.test.tool.input.EventTimeInput;

import java.util.ArrayList;
import java.util.List;

/**
 * A EventTimeInputTranslator transforms an {@link EventTimeInput} object into an
 * {@link EventTimeInput} of another type.
 * <p>
 * Implement this interface to translate concise {@link EventTimeInput} to the type of input
 * required by your test.
 * E.g: translate from tuples into json strings.
 *
 * @param <IN>
 * @param <OUT>
 */
public abstract class EventTimeInputTranslator<IN, OUT> implements EventTimeInput<OUT> {

	private final EventTimeInput<IN> input;

	protected EventTimeInputTranslator(EventTimeInput<IN> input) {
		this.input = input;
	}

	abstract protected OUT translate(IN elem);

	@Override
	public List<StreamRecord<OUT>> getInput() {
		return translateInput(input);
	}

	private List<StreamRecord<OUT>> translateInput(final EventTimeInput<IN> input) {
		List<StreamRecord<OUT>> out = new ArrayList<>();
		for (StreamRecord<IN> elem : input.getInput()) {
			out.add(new StreamRecord<OUT>(
							translate(elem.getValue()),
							elem.getTimestamp())
			);
		}
		return out;
	}
}
