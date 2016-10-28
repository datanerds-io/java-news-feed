package io.datanerds.newsfeed.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class BlockingQueueProcessor<K, V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueProcessor.class);
    private final BlockingQueue<ConsumerRecord<K, V>> queue;
    private volatile boolean stopped = false;
    private final Action<V> consumer;
    private final Consumer<K, V> kafkaConsumer;
    private final ExceptionHandler exceptionHandler;

    public BlockingQueueProcessor(Consumer<K, V> kafkaConsumer, Action consumer,
            BlockingQueue<ConsumerRecord<K, V>> queue, ExceptionHandler exceptionHandler) {
        this.consumer = consumer;
        this.kafkaConsumer = kafkaConsumer;
        this.queue = queue;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public void run() {
        try {
            Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
            while (!stopped) {
                ConsumerRecord<K, V> record = queue.take();
                consumer.apply(record.value());
                offset.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset()));

                kafkaConsumer.commitAsync(offset, (offsets, ex) -> {
                    if (ex != null) {
                        logger.error("Something went wrong during offset commit", ex);
                    } else {
                        logger.debug("Committed some offset");
                    }
                });
            }
            kafkaConsumer.commitSync(offset);
        } catch (InterruptedException ignored) {
            //
        } catch (Exception ex) {
            logger.error("Something went wrong", ex);
            exceptionHandler.handle(ex);
            throw ex;
        }
    }

    public void stop() {
        this.stopped = true;
    }
}
