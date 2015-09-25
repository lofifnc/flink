package output;

import org.scalatest.exceptions.TestFailedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class OutputVerifier<OUT> {

	OutputListener<OUT> listener;
	OutputMatcher<OUT> matcher;

	public OutputVerifier(OutputMatcher<OUT> matcher, int port) {
		listener = new OutputListener<>(port);
		this.matcher = matcher;
	}

	//TODO throw TestFailedException
	public void verify() {
		try {
			List<OUT> output = listener.getOutput();
			matcher.check(output);
		} catch (ExecutionException | InterruptedException e) {
			throw new TestFailedException(1);
		}
	}

}
