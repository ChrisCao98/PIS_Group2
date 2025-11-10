
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import config.KafkaConfig;

import java.time.Duration;
import java.util.*;
import java.util.Scanner;

/**
 * Example Kafka consumer application that allows users to:
 * 1. Subscribe to multiple topics (test-data, test-image, test-user-input)
 * 2. Load configuration from external properties file
 * 3. Process messages from selected topics continuously
 */
public class KafkaConsumerExample {
    public static void main(String[] args) {
        // Load configuration from properties file and other sources
        KafkaConfig config = new KafkaConfig(args);
        
        // Set up Kafka consumer properties with configured values
        Properties props = new Properties();
        // Configure connection to Kafka cluster
        props.put("bootstrap.servers", config.getBootstrapServers());
        props.put("group.id", config.getGroupId());
        // Configure serializers for key and value
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Create Kafka consumer instance with configured properties
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        // Initialize interactive topic selection
        Scanner scanner = new Scanner(System.in);
        List<String> selectedTopics = new ArrayList<>();
        
        // Display available topics for subscription
        System.out.println("Available topics:");
        System.out.println("1. test-data");
        System.out.println("2. test-image");
        System.out.println("3. test-user-input");
        System.out.println("\nEnter topic numbers to subscribe (e.g., '1 2' for first two topics):");
        
        // Get and parse user input
        String input = scanner.nextLine();
        // Split input by whitespace and remove empty strings
        String[] choices = input.trim().split("\\s+");
        
        // Process each topic choice
        for (String choice : choices) {
            try {
                switch (Integer.parseInt(choice)) {
                    case 1:
                        selectedTopics.add("test-data");
                        break;
                    case 2:
                        selectedTopics.add("test-image");
                        break;
                    case 3:
                        selectedTopics.add("test-user-input");
                        break;
                    default:
                        System.out.println("Invalid choice ignored: " + choice);
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid input ignored: " + choice);
            }
        }
        
        if (selectedTopics.isEmpty()) {
            System.out.println("No valid topics selected. Using test-data as default.");
            selectedTopics.add("test-data");
        }
        
        System.out.println("Subscribing to topics: " + String.join(", ", selectedTopics));
        consumer.subscribe(selectedTopics);

        // Start consuming messages in an infinite loop
        try {
            while (true) {
                // Poll for new messages every 100ms
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                // Process each received record
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received message: key = " + record.key() + ", value = " + record.value());
                    // Messages can be saved to file or processed further here
                }
            }
        } finally {
            consumer.close();
            scanner.close();
        }
    }
}