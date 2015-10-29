package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.streaming.test.tool.output.SimpleOutputVerifier;
import org.hamcrest.Matcher;
import org.junit.Assert;

import java.util.List;

public class HamcrestVerifier<T> extends SimpleOutputVerifier<T> {

	public final Matcher<Iterable<T>> matcher;

	public HamcrestVerifier(Matcher<Iterable<T>> matcher) {
		this.matcher = matcher;
	}

	@Override
	public void verify(List<T> output) {
		Assert.assertThat(output,matcher);
	}
}
