package org.apache.flink.streaming.test.tool.core.input;

import java.util.concurrent.TimeUnit;

/**
 * Helper for defining a time span between to StreamRecords
 */
public class Before implements TimeSpan {

	private long timeSpan;

	public static Before period(long time, TimeUnit timeUnit) {
		return new Before(time, timeUnit);
	}

	private Before(long time, TimeUnit timeUnit) {
		this.timeSpan = timeUnit.toMillis(time);
	}

	/**
	 * Getter for defined time span
	 *
	 * @return time span in milliseconds
	 */
	public long getTimeSpan() {
		return timeSpan * -1;
	}
}
