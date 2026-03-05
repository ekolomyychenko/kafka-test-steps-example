package kafka.test.steps.example;

import org.apache.kafka.clients.admin.NewTopic;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;

public class ConfigHelper {

    public static Collection<NewTopic> getKafkaTopics() throws IOException {
        Collection<NewTopic> collection = new ArrayList<>();

        InputStream is = ConfigHelper.class.getResourceAsStream("/kafka-topics");
        try (Scanner scanner = new Scanner(is)) {
            while (scanner.hasNextLine()) {
                collection.add(new NewTopic(scanner.nextLine(), 1, (short) 1));
            }
        }
        return collection;
    }

}
