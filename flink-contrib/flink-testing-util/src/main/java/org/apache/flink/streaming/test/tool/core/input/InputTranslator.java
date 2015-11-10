package org.apache.flink.streaming.test.tool.core.input;

import org.apache.flink.streaming.test.tool.input.EventTimeInput;
import org.apache.flink.streaming.test.tool.input.Input;

import java.util.ArrayList;
import java.util.List;

/**
 * A InputTranslator transforms an {@link Input} object into an
 * {@link Input} of another type.
 * <p>
 * Implement this interface to translate concise {@link Input} to the type of input
 * required by your test.
 * E.g: translate from tuples into json strings.
 *
 * @param <IN>
 * @param <OUT>
 */
public abstract class InputTranslator<IN,OUT> implements Input<OUT> {

	private Input<IN> input;

	protected InputTranslator(Input<IN> input) {
		this.input = input;
	}

	abstract protected OUT translate(IN elem);

	@Override
	public List<OUT> getInput() {
		List<OUT> out = new ArrayList<OUT>();
		for (IN elem: input.getInput()) {
			out.add(translate(elem));
		}
		return out;
	}
}
