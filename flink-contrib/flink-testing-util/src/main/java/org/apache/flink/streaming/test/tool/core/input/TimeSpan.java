package org.apache.flink.streaming.test.tool.core.input;

public interface TimeSpan {

	/**
	 * Getter for defined time span
	 *
	 * @return time span in milliseconds
	 */
	public long getTimeSpan();
}
