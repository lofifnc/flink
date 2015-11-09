package org.apache.flink.streaming.test.tool.core;

import org.apache.flink.streaming.test.tool.core.input.After;
import org.apache.flink.streaming.test.tool.core.input.Before;
import org.apache.flink.streaming.test.tool.core.input.EventTimeInputBuilder;
import org.apache.flink.streaming.test.tool.input.EventTimeInput;

import java.util.concurrent.TimeUnit;

public class Sugar {

	public static After after(long span,TimeUnit unit) {
		return After.period(span,unit);
	}


	public static Before before(long span,TimeUnit unit) {
		return Before.period(span, unit);
	}

	public static <T> EventTimeInputBuilder<T> fromInput(T elem) {
		return EventTimeInputBuilder.create(elem);
	}

	public static int times(int n) {
		return n;
	}

	public static final TimeUnit seconds = TimeUnit.SECONDS;
	public static final TimeUnit minutes = TimeUnit.MINUTES;
	public static final TimeUnit hours = TimeUnit.HOURS;
}
