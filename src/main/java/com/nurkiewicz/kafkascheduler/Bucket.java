package com.nurkiewicz.kafkascheduler;

public record Bucket(int index) {

	@Override
	public String toString() {
		return String.valueOf(index);
	}

	public boolean isFirst() {
		return index == 0;
	}

}
