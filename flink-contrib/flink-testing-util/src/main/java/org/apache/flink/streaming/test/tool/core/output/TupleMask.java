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

public class TupleMask<T extends Tuple> {
	private String[] keys;

	public TupleMask(String... cols) {
		keys = cols;
	}

	public TupleMap<T> convert(T tuple) {
		return new TupleMap<T>(tuple,keys);
	}

	public static <T extends Tuple> TupleMask<T> create(String... cols){
		return new TupleMask<>(cols);
	}

}
