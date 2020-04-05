package kafka.test.steps.example;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class KafkaClient {


    public static String BOOTSTRAP_SERVERS;


    private Properties buildConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return props;
    }

    public Consumer createConsumer(String topic) {
        Properties props = buildConsumerProps();
        Consumer consumer = new KafkaConsumer(props, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private Properties buildAdminClientProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("client.id", UUID.randomUUID().toString());
        props.put("group.id", UUID.randomUUID().toString());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", true);
        return props;
    }


    public void createTopics() throws ExecutionException, InterruptedException, IOException {
        Properties props = buildAdminClientProps();
        Collection<NewTopic> collection = ConfigHelper.getKafkaTopics();
        AdminClient adminClient = AdminClient.create(props);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(collection);
        createTopicsResult.all().get();
    }

    private Properties buildProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        return props;
    }

    public Producer createProducer() {
        Properties props = buildProducerProps();
        return new KafkaProducer(props, new StringSerializer(), new StringSerializer());
    }

    public void send(Producer<String, String> producer, String topic, String event) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord(topic, UUID.randomUUID().toString(), event);
        producer.send(record).get();
    }


    public List<String> getMessages(Consumer<String, String> consumer) {
        List<String> events = new ArrayList<>();

        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            events.add(record.value());
        }

        consumer.commitSync();
        consumer.close();
        return events;
    }

}