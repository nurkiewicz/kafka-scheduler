package com.nurkiewicz.kafkascheduler;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

@RequiredArgsConstructor
@Slf4j
class BucketScanner implements Runnable {

	private final SchedulerConfig cfg;
	private final TimeRanges timeRanges;
	private final Bucket bucket;

	@Override
	public void run() {
		try (KafkaConsumer<byte[], byte[]> consumer = consumer()) {
			TopicPartition timeBucket = new TopicPartition(cfg.getTopic(), bucket.index());
			consumer.assign(List.of(timeBucket));
			while (!Thread.currentThread().isInterrupted()) {
				ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(10));
				for (ConsumerRecord<byte[], byte[]> record : records) {
					log.info("Processing {}", record);
//					producer.send(new ProducerRecord<>(topic, partition, record.key(), record.value()));
				}
				sleep();
			}
		} catch(InterruptException | InterruptedException ignored) {
		}
		log.info("Shutting down");
	}

	private void sleep() throws InterruptedException {
		Duration duration = timeRanges.startThresholdFor(bucket);
		if (duration.isZero()) {
			duration = Duration.ofMillis(100);
		}
		log.trace("Sleeping for {}ms ({})", duration.toMillis(), duration);
		TimeUnit.NANOSECONDS.sleep(duration.toNanos());
	}

	private KafkaConsumer<byte[], byte[]> consumer() {
		Properties consumerProps = new Properties();
		consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getBootstrapServers());
		consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, cfg.getGroupId());
		consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new KafkaConsumer<>(consumerProps);
	}

}
