package com.nurkiewicz.kafkascheduler;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
@RequiredArgsConstructor
public class KafkaScheduler implements MessageScheduler, AutoCloseable {

	private final List<Thread> scanners = new CopyOnWriteArrayList<>();
	private final SchedulerConfig cfg;
	private TimeRanges timeRanges;

	void start() {
		log.info("Starting. Configuration={}", cfg);
		try (KafkaConsumer<String, String> consumer = consumer()) {
			int count = consumer.partitionsFor(cfg.getTopic()).size();
			log.info("Starting workers. Partitions={}", count);
			timeRanges = new TimeRanges(count);
			scanners.addAll(buildScanners(count));
		}

	}

	private List<Thread> buildScanners(int count) {
		return IntStream.range(0, count).mapToObj(this::timeBucketScanner).toList();
	}

	private Thread timeBucketScanner(int index) {
		Thread thread = new Thread(new BucketScanner(cfg, timeRanges, index), "Kafka-scheduler-Bucket-" + index);
		thread.start();
		return thread;
	}

	private KafkaConsumer<String, String> consumer() {
		Properties consumerProps = new Properties();
		consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getBootstrapServers());
		consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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
