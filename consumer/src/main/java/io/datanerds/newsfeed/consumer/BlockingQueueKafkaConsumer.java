package io.datanerds.newsfeed.consumer;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class BlockingQueueKafkaConsumer<K, V> implements ExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueKafkaConsumer.class);
    private static final int QUEUE_SIZE = 2 ^ 8;
    private final String topic;
    private final String brokerBootstrap;
    private final String groupId;
    private final ExecutorService pool;
    private final Class<? extends Deserializer<K>> keyDeserializer;
    private final Class<? extends Deserializer<V>> valueDeserializer;
    private final Consumer<K, V> kafkaConsumer;
    private final Action<V> action;
    private final List<BlockingQueue<ConsumerRecord<K, V>>> queues;
    private volatile KafkaRecordRelay<K, V> kafkaRelay;

    public BlockingQueueKafkaConsumer(String brokerBootstrap, String groupId, String topic,
            Class<? extends Deserializer<K>> keyDeserializer, Class<? extends Deserializer<V>> valueDeserializer,
            Action<V> action) {
        this.brokerBootstrap = brokerBootstrap;
        this.groupId = groupId;
        this.topic = topic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.action = action;

        this.kafkaConsumer = createConsumer();
        this.kafkaConsumer.subscribe(Lists.newArrayList(topic));
        Set<TopicPartition> partitions = kafkaConsumer.assignment();

        this.queues = new ArrayList<>(partitions.size());
        this.pool = Executors.newFixedThreadPool(partitions.size());
        partitions.forEach(partition -> createProcessor());
    }

    @Override
    public void handle(Exception ex) {
        logger.error("Stopping all apply threads due to unexpected exception in processing threads", ex);
        stop();
    }

    public void relay(ConsumerRecord<K, V> message) throws InterruptedException {
        if (topic.equals(message.topic())) {
            throw new FancyException(String.format("Message from unexpected topic: '%'", message.topic()));
        }
        BlockingQueue<ConsumerRecord<K, V>> queue = queues.get(message.partition());
        queue.put(message);
    }

    public void start() {
        synchronized (kafkaRelay) {
            if (kafkaRelay != null) {
                throw new FancyException("Sry, already started");
            }
            kafkaRelay = new KafkaRecordRelay<>(kafkaConsumer, this);
            new Thread(this.kafkaRelay).start();
        }
    }

    public void stop() {
        synchronized (kafkaRelay) {
            if (kafkaRelay == null) {
                throw new FancyException("Not started, nothing to stop here");
            }
            try {
                kafkaRelay.stop();
                pool.shutdown();
                pool.awaitTermination(10, SECONDS);
            } catch (InterruptedException ex) {
                logger.error("Could not kill the pool, hey", ex);
            }
        }
    }

    private void createProcessor() {
        ArrayBlockingQueue<ConsumerRecord<K, V>> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        BlockingQueueProcessor<K, V> processor = new BlockingQueueProcessor<>(kafkaConsumer, this.action, queue, this);
        pool.execute(processor);
    }

    private Consumer<K, V> createConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, brokerBootstrap);
        props.put(GROUP_ID_CONFIG, groupId);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(CLIENT_ID_CONFIG, getClientId());
        return new KafkaConsumer<>(props);
    }

    private String getClientId() {
        try {
            return String.format("%s-%s", InetAddress.getLocalHost().getHostName(), topic);
        } catch (UnknownHostException ex) {
            throw new ConsumerException("Could not retrieve client identifier", ex);
        }
    }
}
