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

}
