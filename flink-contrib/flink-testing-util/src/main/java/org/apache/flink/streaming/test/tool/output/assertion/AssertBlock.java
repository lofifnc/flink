/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.output.assertion;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.KeyMatcherPair;
import org.apache.flink.streaming.test.tool.core.output.TupleMask;
import org.apache.flink.streaming.test.tool.output.assertion.tuple.TupleMapMatchers;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

/**
 * Enables the use of a {@link TupleMask} to map a {@link Tuple} to string keys.
 * And then use these keys in combination with hamcrest {@link Matcher}s.
 *
 * @param <T>
 */
public class AssertBlock<T extends Tuple> {

	private final TupleMask<T> mask;
	private List<KeyMatcherPair> matchers = new ArrayList<>();

	/**
	 * Constructor
	 *
	 * @param fields list of keys
	 */
	public AssertBlock(String... fields) {
		this(new TupleMask<T>(fields));
	}

	/**
	 * Constructor
	 *
	 * @param mask to use
	 */
	public AssertBlock(TupleMask<T> mask) {
		this.mask = mask;
	}

	public static <T extends Tuple> AssertBlock<T> fromMask(TupleMask<T> mask) {
		return new AssertBlock<T>(mask);
	}

	/**
	 * Add a new assertion to the list.
	 *
	 * @param key   of the field
	 * @param match matcher to use on the field
	 * @return
	 */
	public AssertBlock<T> assertThat(String key, Matcher match) {
		matchers.add(KeyMatcherPair.of(key, match));
		return this;
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> anyOfThem() {
		return new ResultMatcher<>(TupleMapMatchers.any(matchers, mask));
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> eachOfThem() {
		return new ResultMatcher<>(TupleMapMatchers.each(matchers, mask));
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> exactlyNOfThem(int n) {
		return new ResultMatcher<>(TupleMapMatchers.exactly(matchers, mask, n));
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> atLeastNOfThem(int n) {
		return new ResultMatcher<>(TupleMapMatchers.atLeast(matchers, mask, n));
	}

	/**
	 * @return
	 */
	public ResultMatcher<T> atMostNOfThem(int n) {
		return new ResultMatcher<>(TupleMapMatchers.atMost(matchers, mask, n));
	}

}
