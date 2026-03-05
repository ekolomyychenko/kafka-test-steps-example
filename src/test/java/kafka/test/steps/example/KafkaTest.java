package kafka.test.steps.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Testcontainers
public class KafkaTest {

    @Container
    public KafkaContainer kafka = new KafkaContainer()
            .withEmbeddedZookeeper();

    private KafkaClient kafkaClient;

    @BeforeEach
    void setUp() {
        kafkaClient = new KafkaClient(kafka.getBootstrapServers());
    }

    @Test
    public void kafka_test() throws ExecutionException, InterruptedException, IOException {
        kafkaClient.createTopics();
        Producer<String, String> producer = kafkaClient.createProducer();
        Consumer<String, String> consumer = kafkaClient.createConsumer("some-test-topic");

        kafkaClient.send(producer, "some-test-topic", "Hello");
        kafkaClient.send(producer, "some-test-topic", "World");

        List<String> messagesActual = kafkaClient.getMessages(consumer);
        List<String> messagesExpected = List.of("Hello", "World");

        Assertions.assertEquals(messagesExpected, messagesActual);
    }
}
