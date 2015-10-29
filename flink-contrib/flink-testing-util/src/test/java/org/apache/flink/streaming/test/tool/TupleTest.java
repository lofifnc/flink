package org.apache.flink.streaming.test.tool;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.test.tool.output.SimpleOutputVerifier;
import org.apache.flink.streaming.test.tool.output.assertion.AssertBlock;
import org.apache.flink.streaming.test.tool.output.assertion.HamcrestVerifier;
import org.apache.flink.streaming.test.tool.output.assertion.OutputMatcher;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class TupleTest {


	@org.junit.Test
	public void test() {

		OutputMatcher<Tuple2<String,Integer>> matcher = new AssertBlock<Tuple2<String,Integer>>("name","age")
				.assertThat("age", greaterThan(40))
				.assertThat("name", is("peter"))
				.assertThat("age", lessThan(50))
				.any().each();

		List<Tuple2<String,Integer>> output = new ArrayList<>();
		output.add(Tuple2.of("hans", 49));
		output.add(Tuple2.of("peter", 45));
		output.add(Tuple2.of("heidi", 43));

		SimpleOutputVerifier<Tuple2<String,Integer>> verifier = new HamcrestVerifier<>(matcher);

		verifier.verify(output);

	}

}

