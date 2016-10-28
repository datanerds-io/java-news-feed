package io.datanerds.newsfeed.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRecordRelay<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRecordRelay.class);
    private static final long POLLING_TIMEOUT_MS = 5000;
    private final Consumer<K, V> kafkaConsumer;
    private final BlockingQueueKafkaConsumer<K, V> consumer;
    private volatile boolean stopped = false;

    public KafkaRecordRelay(Consumer<K, V> kafkaConsumer, BlockingQueueKafkaConsumer<K, V> consumer) {
        this.kafkaConsumer = kafkaConsumer;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        while (!stopped) {
            ConsumerRecords<K, V> records = kafkaConsumer.poll(POLLING_TIMEOUT_MS);
            records.forEach(record -> {
                try {
                    consumer.relay(record);
                } catch (InterruptedException ignored) {
                    //
                } catch (Exception ex) {
                    consumer.handle(ex);
                }
            });
        }
        logger.info("Kafka message relay stopped");
    }

    void stop() {
        logger.info("Stopping Kafka message relay");
        stopped = true;
    }
}
