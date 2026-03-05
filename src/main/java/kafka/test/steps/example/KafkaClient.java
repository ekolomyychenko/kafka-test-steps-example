package kafka.test.steps.example;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Log4j2
public class KafkaClient {

    private final String bootstrapServers;

    public KafkaClient(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private Properties buildConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return props;
    }

    public Consumer<String, String> createConsumer(String topic) {
        Properties props = buildConsumerProps();
        Consumer<String, String> consumer = new KafkaConsumer<>(
                props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private Properties buildAdminClientProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        return props;
    }

    public void createTopics() throws ExecutionException, InterruptedException, IOException {
        Properties props = buildAdminClientProps();
        Collection<NewTopic> collection = ConfigHelper.getKafkaTopics();
        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(collection).all().get();
        }
    }

    private Properties buildProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        return props;
    }

    public Producer<String, String> createProducer() {
        Properties props = buildProducerProps();
        return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    }

    public void send(Producer<String, String> producer, String topic, String event)
            throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, UUID.randomUUID().toString(), event);
        producer.send(record).get();
        log.info("Send message with producer {}:\n{}", producer, record);
    }

    public List<String> getMessages(Consumer<String, String> consumer) {
        List<String> events = new ArrayList<>();
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        records.forEach(record -> events.add(record.value()));
        consumer.commitSync();
        consumer.close();
        log.info("Get messages with consumer {}:\n{}", consumer, events);
        return events;
    }

}