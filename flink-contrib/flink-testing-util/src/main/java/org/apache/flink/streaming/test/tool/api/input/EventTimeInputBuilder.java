/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.api.input;

import org.apache.flink.streaming.test.tool.input.EventTimeInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder to define the input for a test
 * Offers multiple methods to generate input with the EventTime attached.
 * @param <T> value type
 */
public class EventTimeInputBuilder<T> implements EventTimeInput<T> {

	/** List of input containing StreamRecords */
	private ArrayList<StreamRecord<T>> input = new ArrayList<>();

	private EventTimeInputBuilder(StreamRecord<T> record) {
		input.add(record);
	}

	/**
	 * Adds the first record to the input
	 * @param elem value
	 * @param timeStamp
	 * @param <T>
	 * @return
	 */
	public static <T> EventTimeInputBuilder<T> create(T elem, long timeStamp) {
		return new EventTimeInputBuilder<T>(new StreamRecord<T>(elem,timeStamp));
	}

	/**
	 * Adds the first StreamRecord to the input
	 * @param record
	 * @param <T>
	 * @return
	 */
	public static <T> EventTimeInputBuilder<T> create(StreamRecord<T> record) {
		return new EventTimeInputBuilder<T>(record);
	}

	/**
	 * Adds the first record to the input using the current time as
	 * first timestamp
	 * @param elem
	 * @param <T>
	 * @return
	 */
	public static <T> EventTimeInputBuilder<T> create(T elem) {
		return new EventTimeInputBuilder<T>(new StreamRecord<T>(elem, 0));
	}

	/**
	 * Add an element with timestamp to the input
	 * @param elem
	 * @param timeStamp
	 * @return
	 */
	public EventTimeInputBuilder<T> add(T elem, long timeStamp) {
		input.add(new StreamRecord<T>(elem, timeStamp));
		return this;
	}

	/**
	 * Add an element with an {@link After} object,
	 * defining the time between the previous and the new record.
	 * @param elem
	 * @param after
	 * @return
	 */
	public EventTimeInputBuilder<T> add(T elem, After after) {
		long lastTimeStamp = input.get(input.size() - 1).getTimestamp();
		long currentTimeStamp = lastTimeStamp + after.getTimeSpan();
		input.add(new StreamRecord<T>(elem, currentTimeStamp));
		return this;
	}

	/**
	 * Add a {@link StreamRecord} to the list of input
	 * @param record
	 * @return
	 */
	public EventTimeInputBuilder<T> add(StreamRecord<T> record) {
		input.add(record);
		return this;
	}

	/**
	 * Print the input list.
	 * @return
	 */
	public String toString(){
		StringBuilder builder = new StringBuilder();
		for(StreamRecord<T> r : input) {
			builder.append("value: " + r.getValue() + " timestamp: " + r.getTimestamp() +"\n");
		}

		return builder.toString();
	}


	@Override
	public List<StreamRecord<T>> getInput() {
		return input;
	}
}
