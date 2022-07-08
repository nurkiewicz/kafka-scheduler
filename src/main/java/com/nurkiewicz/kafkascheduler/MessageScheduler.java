package com.nurkiewicz.kafkascheduler;

import java.time.Duration;

public interface MessageScheduler extends AutoCloseable{

	void sendLater(byte[] key, byte[] value, String topic, Duration delay);

}
