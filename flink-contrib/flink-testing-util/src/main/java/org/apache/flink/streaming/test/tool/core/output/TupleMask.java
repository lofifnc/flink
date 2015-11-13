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

package org.apache.flink.streaming.test.tool.core.output;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.test.tool.core.TupleMap;


/**
 * Defines a mask of string keys applied to a {@link Tuple}.
 * @param <T>
 */
public class TupleMask<T extends Tuple> {
	private String[] keys;

	public TupleMask(String... keys) {
		this.keys = keys;
	}

	/**
	 * Converts a {@link Tuple} into a {@link TupleMap}
	 * using the mask as keys.
	 *
	 * @param tuple {@link Tuple} to convert.
	 * @return {@link TupleMap} with applied keys.
	 */
	public TupleMap<T> convert(T tuple) {
		return new TupleMap<T>(tuple,keys);
	}

	/**
	 * Factory method.
	 *
	 * @param cols list of Strings.
	 * @param <T> {@link TupleMask} type.
	 * @return new instance of this class.
	 */
	public static <T extends Tuple> TupleMask<T> create(String... cols){
		return new TupleMask<>(cols);
	}

}
