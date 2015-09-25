package api.output;

import output.OutputMatcher;

import java.util.ArrayList;
import java.util.List;

public class ExpectedOutput<OUT> {

	List<OUT>  output;
	public OutputMatcher<OUT> expect;

	public ExpectedOutput() {
		expect = new OutputMatcher<>();
		output = new ArrayList<>();
	}

	public ExpectedOutput<OUT> add(OUT elem) {
		output.add(elem);
		return this;
	}

	public OutputMatcher<OUT> getMatcher() {
		expect.addExpectedOutput(output);
		return expect;
	}

}
