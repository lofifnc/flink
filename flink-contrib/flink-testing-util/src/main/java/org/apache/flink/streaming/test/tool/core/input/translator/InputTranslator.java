package org.apache.flink.streaming.test.tool.core.input.translator;

import org.apache.flink.streaming.test.tool.input.Input;

import java.util.ArrayList;
import java.util.List;

public abstract class InputTranslator<IN,OUT> implements Input<OUT> {

	private Input<IN> input;

	public InputTranslator(Input<IN> input) {
		this.input = input;
	}

	abstract public OUT translate(IN elem);

	@Override
	public List<OUT> getInput() {
		List<OUT> out = new ArrayList<OUT>();
		for (IN elem: input.getInput()) {
			out.add(translate(elem));
		}
		return out;
	}
}
