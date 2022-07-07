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
import org.apache.kafka.common.serialization.StringDeserializer;

@RequiredArgsConstructor
@Slf4j
class BucketScanner implements Runnable {

	private final SchedulerConfig cfg;
	private final TimeRanges timeRanges;
	private final int index;

	@Override
	public void run() {
		try (KafkaConsumer<String, String> consumer = consumer()) {
			TopicPartition timeBucket = new TopicPartition(cfg.getTopic(), index);
			consumer.assign(List.of(timeBucket));
			while (!Thread.currentThread().isInterrupted()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
				for (ConsumerRecord<String, String> record : records) {
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
		Duration duration = timeRanges.forBucket(index);
		log.trace("Sleeping for {}", duration);
		TimeUnit.NANOSECONDS.sleep(duration.toNanos());
	}

	private KafkaConsumer<String, String> consumer() {
		Properties consumerProps = new Properties();
		consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getBootstrapServers());
		consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, cfg.getGroupId());
		consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new KafkaConsumer<>(consumerProps);
	}

}
