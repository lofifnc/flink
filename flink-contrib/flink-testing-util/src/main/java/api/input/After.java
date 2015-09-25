package api.input;

import java.util.concurrent.TimeUnit;

public class After {
	private long period;

	public static After period(long time, TimeUnit timeUnit){
	 return new After(time,timeUnit);
	}

	private After(long time,TimeUnit timeUnit) {
		this.period = timeUnit.toMillis(time);
	}

	public long getPeriod() {
		return period;
	}
}
