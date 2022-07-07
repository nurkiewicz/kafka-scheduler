package com.nurkiewicz.kafkascheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
@RequiredArgsConstructor
public class KafkaScheduler implements MessageScheduler, AutoCloseable {

	private final List<Duration> timeRanges = new CopyOnWriteArrayList<>();
	private final List<Thread> scanners = new CopyOnWriteArrayList<>();
	private final SchedulerConfig cfg;

	void start() {
		log.info("Starting. Configuration={}", cfg);
		try (KafkaConsumer<String, String> consumer = consumer()) {
			int count = consumer.partitionsFor(cfg.getTopic()).size();
			log.info("Starting workers. Partitions={}", count);
			scanners.addAll(buildScanners(count));
			timeRanges.addAll(IntStream.range(0, count).mapToObj(bucket -> Duration.ofSeconds((int) Math.round(Math.pow(2, bucket)))).toList());
		}

	}

	private List<Thread> buildScanners(int count) {
		return IntStream.range(0, count).mapToObj(bucket -> timeBucketScanner(cfg.getTopic(), bucket)).toList();
	}

	private Thread timeBucketScanner(String topic, int partition) {
		Thread thread = new Thread(() -> pollBlocking(topic, partition), "Kafka-scheduler-Bucket-" + partition);
		thread.start();
		return thread;
	}

	private void pollBlocking(String topic, int partition) {
		try (KafkaConsumer<String, String> consumer = consumer()) {
			TopicPartition timeBucket = new TopicPartition(topic, partition);
			consumer.assign(List.of(timeBucket));
			while (!Thread.currentThread().isInterrupted()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					log.info("Processing {}", record);
//					producer.send(new ProducerRecord<>(topic, partition, record.key(), record.value()));
				}
			}
		} catch(org.apache.kafka.common.errors.InterruptException ignored) {
		}
		log.info("Shutting down");
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

	private KafkaProducer<String, String> producer() {
		Properties producerProps = new Properties();
		producerProps.put("retries", 3);
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getBootstrapServers());
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

	@Override
	public void sendLater(byte[] key, byte[] value, String topic, Instant when) {

	}

	@Override
	public void close() {
		log.info("Closing");
		scanners.forEach(Thread::interrupt);
	}
}
