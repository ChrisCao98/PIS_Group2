package config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Configuration management class responsible for loading settings from multiple sources:
 * 1. Configuration file (application.properties)
 * 2. System properties (-D parameters)
 * 3. Environment variables
 * 4. Command line arguments
 *
 * Priority order: Command line > Environment variables > System properties > Properties file > Default values
 */
public class KafkaConfig {
    private final Properties properties = new Properties();
    
    /**
     * Initializes configuration by loading from all available sources
     * @param args Command line arguments in format --key=value
     */
    public KafkaConfig(String[] args) {
        // 1. Load default configuration
        loadDefaultConfig();
        
        // 2. Load from properties file
        loadFromFile("config/application.properties");
        
        // 3. Load from system properties and environment
        loadFromSystemAndEnv();
        
        // 4. Load from command line arguments (format: --key=value)
        loadFromCommandLine(args);
    }

    /**
     * Sets up default configuration values
     * These values are used if not overridden by other configuration sources
     */
    private void loadDefaultConfig() {
        // Kafka settings
        properties.setProperty("kafka.topic.name", "test-data");
        properties.setProperty("kafka.bootstrap.servers", "localhost:9092");
        properties.setProperty("kafka.group.id", "test-consumer-group");
        
        // CSV file settings
        properties.setProperty("csv.file.path", "data/gps_info.csv");
        properties.setProperty("csv.encoding", "UTF-8");
        
        // Batch processing settings
        properties.setProperty("batch.size", "2");
        properties.setProperty("batch.delay.ms", "1000");
    }

    /**
     * Loads configuration from a properties file
     * @param path Path to the properties file
     */
    /**
     * Loads configuration from a properties file
     * @param path Path to the properties file
     */
    private void loadFromFile(String path) {
        try (InputStream input = new FileInputStream(path)) {
            properties.load(input);
        } catch (IOException e) {
            System.err.println("Failed to load configuration from " + path + ", continuing with defaults. Error: " + e.getMessage());
        }
    }

    /**
     * Loads configuration from system properties and environment variables.
     * System properties take precedence over file-based configuration.
     * Environment variables are converted to uppercase with dots replaced by underscores.
     */
    private void loadFromSystemAndEnv() {
        // Check each property for system property and environment variable overrides
        for (String key : properties.stringPropertyNames()) {
            // System properties take precedence
            String sysValue = System.getProperty(key);
            if (sysValue != null) {
                properties.setProperty(key, sysValue);
            }
            
            // Environment variables use uppercase with underscores
            String envKey = key.toUpperCase().replace('.', '_');
            String envValue = System.getenv(envKey);
            if (envValue != null) {
                properties.setProperty(key, envValue);
            }
        }
    }

    /**
     * Loads configuration from command line arguments
     * Accepts arguments in the format: --key=value
     * Command line arguments have the highest priority
     *
     * @param args Command line arguments array
     */
    private void loadFromCommandLine(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String[] parts = arg.substring(2).split("=", 2);
                if (parts.length == 2) {
                    properties.setProperty(parts[0], parts[1]);
                }
            }
        }
    }

    // Kafka Topic Configuration
    
    /**
     * Gets the main Kafka topic name
     * @return The configured Kafka topic name
     */
    public String getKafkaTopicName() {
        return properties.getProperty("kafka.topic.name");
    }

    /**
     * Gets the Kafka topic name for image data
     * @return The configured image topic name, defaults to "test-image"
     */
    public String getImageTopicName() {
        return properties.getProperty("kafka.image.topic.name", "test-image");
    }

    /**
     * Gets the Kafka topic name for user input data
     * @return The configured user input topic name, defaults to "test-user-input"
     */
    public String getUserInputTopicName() {
        return properties.getProperty("kafka.userinput.topic.name", "test-user-input");
    }

    /**
     * Gets the Kafka bootstrap servers configuration
     * @return The configured bootstrap servers list
     */
    public String getBootstrapServers() {
        return properties.getProperty("kafka.bootstrap.servers");
    }

    /**
     * Gets the Kafka consumer group ID
     * @return The configured consumer group ID
     */
    public String getGroupId() {
        return properties.getProperty("kafka.group.id");
    }

    // File System Configuration

    /**
     * Gets the path to the image folder
     * @return The configured image folder path, defaults to "data/img_resize"
     */
    public String getImageFolderPath() {
        return properties.getProperty("image.folder.path", "data/img_resize");
    }

    /**
     * Gets the path to the CSV file
     * @return The configured CSV file path
     */
    public String getCsvFilePath() {
        return properties.getProperty("csv.file.path");
    }

    /**
     * Gets the encoding for CSV file reading
     * @return The configured CSV file encoding, defaults to UTF-8
     */
    public Charset getCsvEncoding() {
        return Charset.forName(properties.getProperty("csv.encoding", "UTF-8"));
    }

    // Network Configuration

    /**
     * Gets the netcat port for user input
     * @return The configured netcat port number, defaults to 7777
     * @throws NumberFormatException if the configured value is not a valid integer
     */
    public int getNcPort() {
        try {
            return Integer.parseInt(properties.getProperty("userinput.nc.port", "7777"));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Invalid netcat port configuration: " + 
                properties.getProperty("userinput.nc.port"));
        }
    }

    // Batch Processing Configuration

    /**
     * Gets the batch size for processing
     * @return The configured batch size, defaults to 2
     * @throws NumberFormatException if the configured value is not a valid integer
     */
    public int getBatchSize() {
        try {
            return Integer.parseInt(properties.getProperty("batch.size", "2"));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Invalid batch size configuration: " + 
                properties.getProperty("batch.size"));
        }
    }

    /**
     * Gets the batch processing delay in milliseconds
     * @return The configured batch delay in ms, defaults to 1000
     * @throws NumberFormatException if the configured value is not a valid integer
     */
    public int getBatchDelayMs() {
        try {
            return Integer.parseInt(properties.getProperty("batch.delay.ms", "1000"));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Invalid batch delay configuration: " + 
                properties.getProperty("batch.delay.ms"));
        }
    }

    /**
     * Prints current configuration values to console
     * Useful for debugging and verifying configuration loading
     */
    public void printConfig() {
        System.out.println("Current Configuration:");
        System.out.println("\nKafka Settings:");
        System.out.println("- Main Topic: " + getKafkaTopicName());
        System.out.println("- Image Topic: " + getImageTopicName());
        System.out.println("- User Input Topic: " + getUserInputTopicName());
        System.out.println("- Bootstrap Servers: " + getBootstrapServers());
        System.out.println("- Consumer Group ID: " + getGroupId());

        System.out.println("\nFile System Settings:");
        System.out.println("- Image Folder: " + getImageFolderPath());
        System.out.println("- CSV File Path: " + getCsvFilePath());
        System.out.println("- CSV Encoding: " + getCsvEncoding());

        System.out.println("\nNetwork Settings:");
        System.out.println("- Netcat Port: " + getNcPort());

        System.out.println("\nBatch Processing Settings:");
        System.out.println("- Batch Size: " + getBatchSize());
        System.out.println("- Batch Delay (ms): " + getBatchDelayMs());
    }
}