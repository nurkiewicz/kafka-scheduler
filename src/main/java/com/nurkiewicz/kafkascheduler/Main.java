package com.nurkiewicz.kafkascheduler;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Main {

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(java.lang.invoke.MethodHandles.lookup().lookupClass());

	public static void main(String[] args) {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "kafka-scheduler";
		String topic = "quickstart";
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//creating consumer
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
			int count = consumer.partitionsFor(topic).size();
			log.info("{} partitions for {}", count, topic);
			TopicPartition timeBucket = new TopicPartition(topic, 12);
			consumer.assign(List.of(timeBucket));
			//polling
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					log.info("Key: " + record.key() + ", Value:" + record.value());
					log.info("Partition:" + record.partition() + ",Offset:" + record.offset());
				}
				consumer.commitSync();
			}
		}
	}
}