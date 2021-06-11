package kafka.test.steps.example;

import org.apache.kafka.clients.admin.NewTopic;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;

public class ConfigHelper {


    public static Collection<NewTopic> getKafkaTopics() throws FileNotFoundException {
        Collection<NewTopic> collection = new ArrayList<>();

        Scanner scanner = new Scanner(new File("src/test/resources/kafka-topics"));
        while (scanner.hasNextLine()) {
            collection.add(new NewTopic(scanner.nextLine(), 1, Short.parseShort("1")));
        }
        return collection;
    }


}
