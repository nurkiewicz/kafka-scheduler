package com.nurkiewicz.kafkascheduler;

import lombok.Builder;
import lombok.Value;
import lombok.With;

@Value
@With
@Builder
public class SchedulerConfig {

	String bootstrapServers;
	String topic;
	String groupId;
	TimeRangeConfig timeRange;

	public static SchedulerConfig defaults() {
		return new SchedulerConfig("127.0.0.1:9092",
				"kafka-scheduler-pending",
				"kafka-scheduler",
				TimeRangeConfig.defaults());
	}

}
