package com.nurkiewicz.kafkascheduler;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TimeRangesTest {

	@Test
	public void smoke() throws Exception{
		//given
		TimeRanges ranges = new TimeRanges(10, new TimeRangeConfig(Duration.ofSeconds(1), 2));

		//then
		assertThat(ranges.startThresholdFor(new Bucket(0))).isEqualTo(Duration.ofSeconds(0));
		assertThat(ranges.startThresholdFor(new Bucket(1))).isEqualTo(Duration.ofSeconds(1));
		assertThat(ranges.startThresholdFor(new Bucket(2))).isEqualTo(Duration.ofSeconds(2));
		assertThat(ranges.startThresholdFor(new Bucket(3))).isEqualTo(Duration.ofSeconds(4));
		assertThat(ranges.startThresholdFor(new Bucket(4))).isEqualTo(Duration.ofSeconds(8));
	}

}