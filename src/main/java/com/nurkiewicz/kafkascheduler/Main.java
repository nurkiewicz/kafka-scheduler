package com.nurkiewicz.kafkascheduler;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

public class Main {

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(java.lang.invoke.MethodHandles.lookup().lookupClass());

	public static void main(String[] args) throws InterruptedException {
		SchedulerConfig cfg = SchedulerConfig.defaults()
				.withTopic("quickstart");
		try (KafkaScheduler scheduler = new KafkaScheduler(cfg)) {
			scheduler.start();
			scheduler.sendLater("k".getBytes(StandardCharsets.UTF_8),
					"v".getBytes(StandardCharsets.UTF_8),
					"target",
					Duration.ofSeconds(10));
			TimeUnit.SECONDS.sleep(60);
		}
	}
}