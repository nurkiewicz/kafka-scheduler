package com.nurkiewicz.kafkascheduler;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

class TimeRanges {

	private final List<Duration> thresholds;

	TimeRanges(int buckets) {
		List<Duration> thresholds = IntStream.range(0, buckets)
				.mapToObj(bucket -> Duration.ofMillis((int) Math.round(1000 * Math.pow(2, bucket))))
				.toList();
		this.thresholds = new CopyOnWriteArrayList<>(thresholds);
	}

	public Duration startThresholdFor(Bucket bucket) {
		if (bucket.isFirst()) {
			return Duration.ZERO;
		}
		return thresholds.get(bucket.index() - 1);
	}

	Bucket forDuration(Duration duration) {
		for (int idx = 0; idx < thresholds.size(); idx++) {
			if (duration.compareTo(thresholds.get(idx)) < 0) {
				return new Bucket(idx);
			}
		}
		return new Bucket(thresholds.size());
	}
}
