package org.apache.flink.streaming.test.tool;

import org.hamcrest.Matcher;

public class KeyMatcherPair {

	public final String key;

	public final Matcher matcher;

	public KeyMatcherPair(String key, Matcher matcher) {
		this.matcher = matcher;
		this.key = key;
	}

	public static KeyMatcherPair of(
			String key,
			Matcher matcher
	) {
		return new KeyMatcherPair(key, matcher);
	}

}
