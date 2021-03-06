package kafka.test.steps.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Testcontainers
public class KafkaTest {

    @Container
    public KafkaContainer kafka = new KafkaContainer()
            .withEmbeddedZookeeper();

    private KafkaClient kafkaClient = new KafkaClient();

    @Test
    public void kafka_test() throws ExecutionException, InterruptedException, IOException {

        KafkaClient.BOOTSTRAP_SERVERS = kafka.getBootstrapServers();
        kafkaClient.createTopics();
        Producer producer = kafkaClient.createProducer();
        Consumer consumer = kafkaClient.createConsumer("some-test-topic");

        kafkaClient.send(producer, "some-test-topic", "Hello");
        kafkaClient.send(producer, "some-test-topic", "World");

        List<String> messagesActual = kafkaClient.getMessages(consumer);
        List<String> messagesExpected = new ArrayList<>(Arrays.asList("Hello", "World"));

        Assertions.assertEquals(messagesExpected, messagesActual);
    }
}
