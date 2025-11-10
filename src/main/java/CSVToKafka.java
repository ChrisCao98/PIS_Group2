/**
 * CSVToKafka - A utility class that reads data from a CSV file and publishes it to a Kafka topic.
 * The class supports batch processing and configurable time intervals between messages.
 * Configuration is loaded from external sources using KafkaConfig.
 */
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import config.KafkaConfig;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class CSVToKafka {
    /** Configuration instance for Kafka and application settings */
    private static KafkaConfig config;

    /**
     * Main entry point of the application.
     * Initializes configuration, reads CSV data, and processes it to Kafka.
     *
     * @param args Command line arguments for configuration
     */
    public static void main(String[] args) {
        // Load configuration
        config = new KafkaConfig(args);
        
        // Print current configuration (optional)
        config.printConfig();
        
        // Read CSV file content
        List<String[]> csvData = readCSVFile();

        // Write CSV data to Kafka
        writeToKafka(csvData);
    }

    /**
     * Reads data from the configured CSV file.
     * Uses OpenCSV library for reliable CSV parsing.
     *
     * @return List of string arrays, where each array represents a row from the CSV file
     */
    private static List<String[]> readCSVFile() {
        List<String[]> csvData = new ArrayList<>();
        try (FileReader fileReader = new FileReader(config.getCsvFilePath());
             CSVReader reader = new CSVReader(fileReader)) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                csvData.add(line);
            }
        } catch (IOException | CsvValidationException e) {
            System.err.println("Failed to read CSV file: " + e.getMessage());
            e.printStackTrace();
        }

        return csvData;
    }

    /**
     * Writes the CSV data to Kafka in an interactive manner.
     * Supports two modes of operation:
     * 1. Batch mode: "run [size]" - Sends specified number of records
     * 2. Interval mode: "run [size] [interval]" - Sends records with time delay between them
     *
     * @param csvData List of CSV records to be sent to Kafka
     */
    private static void writeToKafka(List<String[]> csvData) {
        // Configure Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Scanner scanner = new Scanner(System.in);
        // Initialize control variables
        int size,          // Batch size for sending records
            interval = 0,  // Time interval between records (in milliseconds)
            k = 0,        // Current position in the CSV data
            upbound;      // Upper bound for current batch
            
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                System.out.println("Enter command (format: run <size> [interval_ms]):");
                String input = scanner.nextLine();
                // Split input by spaces to parse command and parameters
                String[] words = input.split(" ");
                // Process "run" command
                if (words[0].equals("run")) {
                    // Handle batch mode: run <size>
                    if (words.length==2){
                        try{
                            size = Integer.parseInt(words[1]);
                        }catch(NumberFormatException e) {
                            System.out.println("Please enter a valid number for size.");
                            continue;
                        }
                    // Handle interval mode: run <size> <interval>
                    } else if (words.length==3) {
                        try{
                            size = Integer.parseInt(words[1]);      // Number of records to send
                            interval = Integer.parseInt(words[2]);   // Delay between records (ms)
                        }catch(NumberFormatException e) {
                            System.out.println("Please enter valid numbers for size and interval.");
                            continue;
                        }
                    }else {
                        System.out.println("Please note the hint.");
                        continue;
                    }
                    // Calculate batch boundaries
                    upbound = Math.min(size + k, csvData.size());
                    
                    // Process and send records in the current batch
                    for (int i = k; i < upbound; i++) {
                        // Convert CSV row to comma-separated string
                        String value = String.join(",", csvData.get(i));
                        // Create and send Kafka record (no key, only value)
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                            config.getKafkaTopicName(), null, value);
                        producer.send(record);
                        producer.flush();
                        if (words.length == 3) {
                            try {
                                Thread.sleep(interval);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                System.err.println("Sending process was interrupted");
                            }
                        }
                    }
                    k += size;
                    if(upbound == csvData.size()){
                        System.out.println("It's done.");
                        break;
                    }
                }else if(words[0].equals("break")){
                    System.out.println("Nothing has happened.");
                    break;
                }else {
                    System.out.println("Invalid input pattern");
                }
            }
        }
        scanner.close();
    }
}