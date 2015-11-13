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

package org.apache.flink.streaming.test.tool.core.input;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.test.tool.runtime.input.EventTimeInput;

import java.util.ArrayList;
import java.util.List;

/**
 * A EventTimeInputTranslator transforms an {@link EventTimeInput} object into an
 * {@link EventTimeInput} of another type.
 * <p>
 * Implement this interface to translate concise {@link EventTimeInput} to the type of input
 * required by your test.
 * E.g: translate from tuples into json strings.
 *
 * @param <IN>
 * @param <OUT>
 */
public abstract class EventTimeInputTranslator<IN, OUT> implements EventTimeInput<OUT> {

	private final EventTimeInput<IN> input;

	protected EventTimeInputTranslator(EventTimeInput<IN> input) {
		this.input = input;
	}

	abstract protected OUT translate(IN elem);

	@Override
	public List<StreamRecord<OUT>> getInput() {
		return translateInput(input);
	}

	private List<StreamRecord<OUT>> translateInput(final EventTimeInput<IN> input) {
		List<StreamRecord<OUT>> out = new ArrayList<>();
		for (StreamRecord<IN> elem : input.getInput()) {
			out.add(new StreamRecord<OUT>(
							translate(elem.getValue()),
							elem.getTimestamp())
			);
		}
		return out;
	}
}
