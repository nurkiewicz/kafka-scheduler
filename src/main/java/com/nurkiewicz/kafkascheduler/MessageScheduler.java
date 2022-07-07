package com.nurkiewicz.kafkascheduler;

import java.time.Instant;

public interface MessageScheduler {

	void sendLater(byte[] key, byte[] value, String topic, Instant when);

}
