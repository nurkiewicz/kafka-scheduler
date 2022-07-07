package com.nurkiewicz.kafkascheduler;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

class TimeRanges {

	private final List<Duration> thresholds;

	public TimeRanges(int buckets) {
		List<Duration> thresholds = IntStream.range(0, buckets)
				.mapToObj(bucket -> Duration.ofMillis((int) Math.round(1000 * Math.pow(2, bucket))))
				.toList();
		this.thresholds = new CopyOnWriteArrayList<>(thresholds);
	}

	public Duration forBucket(int index) {
		return thresholds.get(index);
	}
}
