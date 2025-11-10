import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import config.KafkaConfig;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;


/**
 * Kafka Producer for PNG images that:
 * 1. Reads PNG images from a configured directory
 * 2. Orders them by numeric part of filenames
 * 3. Sends them to Kafka in batches with configurable intervals
 */
public class OrderedPNGImageProducer {
    public static void main(String[] args) {
        // Load configuration from properties file and other sources
        KafkaConfig config = new KafkaConfig(args);
        
        // Set up Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        // Get topic name from configuration
        String topic = config.getImageTopicName();

        // Create Kafka producer
        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        Scanner scanner = new Scanner(System.in);
        int size, interval = 0, k = 0, upbound;

        try {
            // Get image folder path from configuration
            String folderPath = config.getImageFolderPath();

            // Read PNG image files from the configured folder
            File folder = new File(folderPath);
            File[] imageFiles = folder.listFiles();

            // Sort the image files based on file name for ordered processing
            List<File> orderedFiles = new ArrayList<>();
            if (imageFiles != null) {
                for (File file : imageFiles) {
                    orderedFiles.add(file);
                }
                orderedFiles.sort(Comparator.comparing(OrderedPNGImageProducer::getNumericOrder));
            }
            
            // Start processing the ordered image files
            if (!orderedFiles.isEmpty()) {
                while(true){
                    System.out.println("Enter command (format: run <size> [interval_ms]):");
                    String input = scanner.nextLine();
                    // Split input by spaces
                    String[] words = input.trim().split("\\s+");
                    if (words[0].equals("run")) {
                        switch (words.length) {
                            case 2:
                                try {
                                    size = Integer.parseInt(words[1]);
                                } catch (NumberFormatException e) {
                                    System.out.println("Please enter a number for size.");
                                    continue;
                                }
                                break;
                            case 3:
                                try {
                                    size = Integer.parseInt(words[1]);
                                    interval = Integer.parseInt(words[2]);
                                } catch (NumberFormatException e) {
                                    System.out.println("Please enter a number for size and interval.");
                                    continue;
                                }
                                break;
                            default:
                                System.out.println("Please note the hint.");
                                continue;
                        }
                        upbound = Math.min(size + k, orderedFiles.size());
                        for (int i = k; i<upbound; i++) {
                            // Read PNG image file
                            BufferedImage image = ImageIO.read(orderedFiles.get(i));

                            // Convert image to byte array
                            ByteArrayOutputStream by_img = new ByteArrayOutputStream();
                            ImageIO.write(image, "png", by_img);
                            byte[] imageData = by_img.toByteArray();

                            // Publish image data to Kafka
                            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, "image-key", imageData);
                            producer.send(record);
                            System.out.println("Image sent to Kafka successfully: " + orderedFiles.get(i).getName());
                            if (words.length==3){
                                try {
                                    Thread.sleep(interval);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        k += size;
                        if(upbound == orderedFiles.size()){
                            System.out.println("It's done.");
                            break;
                        }
                    }else if(words[0].equals("break")){
                        System.out.println("Nothing has happened.");
                        break;
                    }else {
                        System.out.println("wrong input for pattern");
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static int getNumericOrder(File file) {
        String fileName = file.getName();
        String numericPart = fileName.replaceAll("[^0-9]", "");

        if (numericPart.isEmpty()) {
            // Assign a large value for files without numeric order
            return Integer.MAX_VALUE;
        } else {
            return Integer.parseInt(numericPart);
        }
    }
}

