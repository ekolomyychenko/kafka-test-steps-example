package kafka.test.steps.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.KafkaContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class KafkaTest {

    @Rule
    public KafkaContainer kafka = new KafkaContainer()
            .withEmbeddedZookeeper();

    private KafkaClient kafkaClient = new KafkaClient();

    @Test
    public void kafka_test() throws ExecutionException, InterruptedException, IOException {

        KafkaClient.BOOTSTRAP_SERVERS = kafka.getBootstrapServers();
        kafkaClient.createTopics();

        Producer producer = kafkaClient.createProducer();
        kafkaClient.send(producer, "some-test-topic", "Hello");
        kafkaClient.send(producer, "some-test-topic", "World");

        Consumer consumer = kafkaClient.createConsumer("some-test-topic");
        List<String> messagesActual = kafkaClient.getMessages(consumer);

        List<String> messagesExpected = new ArrayList<>();
        messagesExpected.add("Hello");
        messagesExpected.add("World");

        Assertions.assertEquals(messagesExpected, messagesActual);
    }
}
