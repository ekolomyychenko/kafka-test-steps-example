package kafka.test.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class KafkaTest {


    @Test
    public void kafka_test() throws ExecutionException, InterruptedException, IOException {
        KafkaContainer kafka = new KafkaContainer()
                .withExposedPorts(9092, 9093, 2181)
                .withEmbeddedZookeeper();

        kafka.start();

        KafkaClient kafkaClient = new KafkaClient();

        KafkaClient.BOOTSTRAP_SERVERS = kafka.getBootstrapServers();

        kafkaClient.createTopics();

        Producer producer = kafkaClient.createProducer();
        kafkaClient.send(producer, "some-test-topic", "Hello");

        Consumer consumer = kafkaClient.createConsumer("some-test-topic");
        List<String> messages = kafkaClient.getMessages(consumer);

        System.out.println(messages);
    }

}
