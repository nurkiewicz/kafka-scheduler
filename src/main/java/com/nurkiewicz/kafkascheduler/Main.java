package com.nurkiewicz.kafkascheduler;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

public class Main {

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(java.lang.invoke.MethodHandles.lookup().lookupClass());

	public static void main(String[] args) throws InterruptedException {
		try (KafkaScheduler scheduler = new KafkaScheduler(new SchedulerConfig("127.0.0.1:9092", "quickstart", "kafka-scheduler"))) {
			scheduler.start();
			TimeUnit.SECONDS.sleep(10);
		}
	}
}