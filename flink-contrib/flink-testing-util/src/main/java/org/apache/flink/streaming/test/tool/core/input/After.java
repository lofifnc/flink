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

package org.apache.flink.streaming.test.tool.core.input;

import java.util.concurrent.TimeUnit;

/**
 * Helper for defining a time span between to StreamRecords
 */
public class After implements TimeSpan {
	private long timeSpan;

	public static After period(long time, TimeUnit timeUnit){
		return new After(time,timeUnit);
	}

	private After(long time,TimeUnit timeUnit) {
		this.timeSpan = timeUnit.toMillis(time);
	}

	/**
	 * Getter for defined time span
	 * @return time span in milliseconds
	 */
	public long getTimeSpan() {
		return timeSpan;
	}
}
