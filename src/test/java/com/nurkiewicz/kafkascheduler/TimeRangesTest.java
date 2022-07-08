package com.nurkiewicz.kafkascheduler;

import java.time.Duration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class TimeRangesTest {

	@ParameterizedTest
	@CsvSource({
			"0, PT0S",
			"1, PT1S",
			"2, PT2S",
			"3, PT4S",
			"4, PT8S",
			"5, PT16S",
			"10, PT512S",
	})
	public void smoke(int bucketIdx, Duration expected) throws Exception{
		//given
		TimeRanges ranges = new TimeRanges(10, new TimeRangeConfig(Duration.ofSeconds(1), 2));

		//then
		assertThat(ranges.startThresholdFor(new Bucket(bucketIdx))).isEqualTo(expected);
	}

}