package api;

import api.output.ExpectedOutput;
import input.EventTimeInput;
import input.Input;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.scalatest.exceptions.TestFailedException;
import output.OutputMatcher;
import output.OutputVerifier;
import output.TestSink;
import runtime.TestingStreamEnvironment;

import java.util.ArrayList;
import java.util.List;

public class StreamTestUtil {

	private TestingStreamEnvironment env;
	private List<OutputVerifier> verifiers;
	private Integer port;

	public StreamTestUtil() {
		env = new TestingStreamEnvironment(1,100);
		verifiers = new ArrayList<>();
		port = 5555;
	}

	/**
	 * Creates a DataStreamSource from an EventTimeInput object.
	 * The DataStreamSource will emit the records with the specified EventTime.
	 * @param input to emit
	 * @param <OUT> type of the emitted records
	 * @return a DataStreamSource generating the input
	 */
	public <OUT> DataStreamSource<OUT> createDataSource(EventTimeInput<OUT> input) {
		return env.fromInput(input);
	}

	/**
	 * Creates a DataStreamSource from an Input object.
	 * @param input to emit
	 * @param <OUT> type of the emitted records
	 * @return a DataStream generating the input
	 */
	public <OUT> DataStreamSource<OUT> createDataSource(Input<OUT> input) {
		return env.fromInput(input);
	}

	/**
	 * Creates a TestSink to verify your job output.
	 * @param matcher which will be used to verify the received records
	 * @param <IN> type of the input
	 * @return the created sink.
	 */
	public <IN> TestSink<IN> createTestSink(OutputMatcher<IN> matcher) {
		verifiers.add(new OutputVerifier<IN>(matcher,port));
		TestSink<IN> sink =  new TestSink<IN>(port);
		port++;
		return sink;
	}

	/**
	 * Creates a TestSink to verify your job output.
	 * @param expectedOutput which will be used to verify the received records
	 * @param <IN> type of the input
	 * @return the created sink.
	 */
	public <IN> TestSink<IN> createTestSink(ExpectedOutput<IN> expectedOutput) {
		return createTestSink(expectedOutput.getMatcher());
	}

	/**
	 * Executes the test and verifies the output received by the sinks
	 */
	public void executeTest() {
		try {
			env.execute();
		} catch (Exception e) {
			throw new TestFailedException(1);
		}
		for(OutputVerifier v: verifiers) {
			v.verify();
		}
	}

}
