package io.datanerds.newsfeed.test;

import io.datanerds.newsfeed.producer.EmbeddedKafkaTest;
import net._01001111.text.LoremIpsum;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerTest extends EmbeddedKafkaTest {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
    private static final LoremIpsum LOREM_IPSUM = new LoremIpsum();
    private static final String TOPIC = "finance_news";
    private static final int NUMBER_OF_MESSAGES = 50;

    @BeforeClass
    public static void setUp() {
        EmbeddedKafkaTest.setUp();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        EmbeddedKafkaTest.tearDown();
    }

    @Test
    public void sendAndReceiveTest() throws InterruptedException {
        createTopic(TOPIC);

        //        TestConsumer newsConsumer = new TestConsumer(TOPIC);
        //        SimpleConsumerPool consumerPool = new SimpleConsumerPool(kafkaConnect, "test_group");
        //        consumerPool.start(Arrays.asList(newsConsumer));
        //
        //        NewsProducer newsProducer = new NewsProducer("Breaking News", TOPIC, kafkaConnect);
        //        logger.info("Sending {} messages", NUMBER_OF_MESSAGES);
        //        long start = System.currentTimeMillis();
        //        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
        //            News news = new News(UUID.randomUUID(), "Prof. Kohle", LOREM_IPSUM.paragraph(), LOREM_IPSUM.paragraph());
        //            newsProducer.sendAsync(news);
        //        }
        //        logger.info("Sent {} messages in {}ms", NUMBER_OF_MESSAGES, System.currentTimeMillis() - start);
        //
        //        await().atMost(5, TimeUnit.SECONDS).until(() -> newsConsumer.getMessageCount() == NUMBER_OF_MESSAGES);
        //
        //        assertThat(newsConsumer.getNews().get(0).author, is(equalTo("Prof. Kohle")));
        //
        //        newsProducer.close();
        //        consumerPool.stop();
    }
}
