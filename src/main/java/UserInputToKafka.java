import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class UserInputToKafka {

    public static void main(String[] args) {
        // Kafka configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "test-user-input";

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Start nc command as a child process
            ProcessBuilder processBuilder = new ProcessBuilder("nc", "-lk", "7777");
            Process process = processBuilder.start();

            // Read the output from nc command
            InputStream inputStream = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                // Send the data to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, line);
                producer.send(record);
                System.out.println("Data sent to Kafka: " + line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
