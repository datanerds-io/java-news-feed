package io.datanerds.newsfeed.consumer;

import io.datanerds.newsfeed.domain.News;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * Simple consumer thread using subscribe pattern and auto-commit feature turned on.
 */
public class SimpleConsumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private static final AtomicInteger CONSUMER_SEQUENCE = new AtomicInteger(0);
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile boolean running = true;
    private static final int POLLING_TIMEOUT_MS = 500;
    private final String bootstrapServers;
    private final String groupId;
    private final NewsConsumer newsConsumer;
    private final String name;
    private Consumer<String, News> consumer;
    private final ExceptionHandler exceptionHandler;

    public SimpleConsumer(String broker, String group, NewsConsumer consumer, ExceptionHandler exceptionHandler) {
        this.bootstrapServers = broker;
        this.groupId = group;
        this.newsConsumer = consumer;
        this.exceptionHandler = exceptionHandler;
        this.name = String.format("%s[%s]", group, CONSUMER_SEQUENCE.getAndIncrement());
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName(name);
            consumer = createConsumer();
            consumer.subscribe(Arrays.asList(newsConsumer.getTopic()));
            logger.info("Started consumer thread {}", name);
            while (running) {
                ConsumerRecords<String, News> records = consumer.poll(POLLING_TIMEOUT_MS);
                records.forEach(record -> relayMessage(record));
            }
        } catch (Exception ex) {
            exceptionHandler.handle(ex);
        } finally {
            close();
        }
    }

    private void close() {
        logger.info("Shutting down consumer thread {}", name);
        if (consumer != null) {
            consumer.close();
        }
        latch.countDown();
        logger.info("Consumer {} successfully shutdown", name);
    }

    public void stop() {
        running = false;
        try {
            latch.await();
        } catch (InterruptedException ignored) {
            logger.warn("Consumer thread interrupted while cleaning up.");
        }
    }

    private void relayMessage(ConsumerRecord<String, News> kafkaRecord) {
        logger.info("Received message with key '{}' and offset '{}' on partition '{}' for topic '{}'",
                kafkaRecord.key(), kafkaRecord.offset(), kafkaRecord.partition(), kafkaRecord.topic());
        newsConsumer.consume(kafkaRecord.value());
    }

    private Consumer<String, News> createConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, NewsDeserializer.class);
        props.put(CLIENT_ID_CONFIG, getClientId());

        return new KafkaConsumer<>(props);
    }

    private String getClientId() {
        try {
            return String.format("%s-%s", InetAddress.getLocalHost().getHostName(), name);
        } catch (UnknownHostException ex) {
            throw new ConsumerException("Could not retrieve client identifier", ex);
        }
    }
}
