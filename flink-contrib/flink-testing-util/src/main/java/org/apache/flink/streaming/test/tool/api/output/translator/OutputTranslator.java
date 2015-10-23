package org.apache.flink.streaming.test.tool.api.output.translator;

import org.apache.flink.streaming.test.tool.output.OutputVerifier;
import org.scalatest.exceptions.TestFailedException;

public abstract class OutputTranslator<IN,OUT> implements OutputVerifier<IN> {

	OutputVerifier<OUT> verifier;

	public OutputTranslator(OutputVerifier<OUT> verifier) {
		this.verifier = verifier;
	}

	public abstract OUT translate(IN record);

	@Override
	public void init() {
		verifier.init();
	}

	@Override
	public void receive(IN elem) throws TestFailedException {
		verifier.receive(translate(elem));
	}

	@Override
	public void finish() throws TestFailedException {
		verifier.finish();
	}
}
