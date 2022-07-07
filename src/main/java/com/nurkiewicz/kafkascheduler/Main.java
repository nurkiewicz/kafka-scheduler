package com.nurkiewicz.kafkascheduler;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

public class Main {

	private static final Logger log = org.slf4j.LoggerFactory.getLogger(java.lang.invoke.MethodHandles.lookup().lookupClass());

	public static void main(String[] args) {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "kafka-scheduler";
		String topic = "quickstart";
		Properties consumerProps = new Properties();
		consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		int partition = 12;
		try (KafkaProducer<String, String> producer = buildProducer(bootstrapServers);
		     KafkaConsumer<String, String> consumer = buildConsumer(bootstrapServers, groupId)) {
			int count = consumer.partitionsFor(topic).size();
			log.info("{} partitions for {}", count, topic);
			TopicPartition timeBucket = new TopicPartition(topic, partition);
			consumer.assign(List.of(timeBucket));
			//polling
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					log.info("Processing {}", record);
					producer.send(new ProducerRecord<>(topic, partition, record.key(), record.value()));
				}
				consumer.commitSync();
			}
		}
	}

	private static KafkaConsumer<String, String> buildConsumer(String bootstrapServers, String groupId) {
		Properties consumerProps = new Properties();
		consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new KafkaConsumer<>(consumerProps);
	}

	private static KafkaProducer<String, String> buildProducer(String bootstrapServers) {
		Properties producerProps = new Properties();
		producerProps.put("retries", 3);
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 120000);
		producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
		producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
		producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024);
		producerProps.put("compression.type", "gzip");
		return new KafkaProducer<>(producerProps);
	}
}