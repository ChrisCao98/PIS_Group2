import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import config.KafkaConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * UserInputToKafka - A Kafka producer that:
 * 1. Listens for user input via netcat on a configured port
 * 2. Forwards received input to a Kafka topic
 * 3. Uses configurable settings from properties file
 */
public class UserInputToKafka {

    public static void main(String[] args) {
        // Load configuration from properties file and other sources
        KafkaConfig config = new KafkaConfig(args);
        
        // Set up Kafka producer properties with configured values
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // Get topic name from configuration
        String topic = config.getUserInputTopicName();

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Start netcat command as a child process with configured port
            ProcessBuilder processBuilder = new ProcessBuilder(
                "nc", "-lk", String.valueOf(config.getNcPort())
            );
            Process process = processBuilder.start();

            // Set up reader for netcat output
            InputStream inputStream = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            System.out.println("Listening for input on port " + config.getNcPort());
            System.out.println("Messages will be sent to topic: " + topic);
            
            String line;
            while ((line = reader.readLine()) != null) {
                // Create and send record to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, line);
                producer.send(record);
                System.out.println("Message sent to Kafka: " + line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
