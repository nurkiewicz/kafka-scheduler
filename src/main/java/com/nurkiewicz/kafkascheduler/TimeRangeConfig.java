package com.nurkiewicz.kafkascheduler;

import java.time.Duration;

import lombok.Builder;
import lombok.Value;
import lombok.With;

@Value
@With
@Builder
public class TimeRangeConfig {
	Duration firstRange;
	double multiplier;

	public static TimeRangeConfig defaults() {
		return new TimeRangeConfig(Duration.ofSeconds(1), 2.0);
	}
}
