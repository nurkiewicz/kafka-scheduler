package com.nurkiewicz.kafkascheduler;

import lombok.Value;
import lombok.With;

@Value
@With
public class SchedulerConfig {

	String bootstrapServers;
	String topic;
	String groupId;

}
