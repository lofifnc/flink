package org.apache.flink.streaming.test.tool.core.input.translator;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.test.tool.input.EventTimeInput;

import java.util.ArrayList;
import java.util.List;

public abstract class EventTimeInputTranslator<IN,OUT> {

	abstract public OUT translate(IN elem);

	public EventTimeInput<OUT> translateInput(final EventTimeInput<IN> input) {

		return new EventTimeInput<OUT>() {

			private final EventTimeInput<IN> eTInput = input;

			@Override
			public List<StreamRecord<OUT>> getInput() {
				List<StreamRecord<OUT>> out = new ArrayList<>();
				for (StreamRecord<IN> elem: eTInput.getInput()) {
					out.add(new StreamRecord<OUT>(
									translate(elem.getValue()),
									elem.getTimestamp())
					);
				}
				return out;
			}
		};
	}
}
