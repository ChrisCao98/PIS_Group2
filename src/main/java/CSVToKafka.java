import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
//It can send data to Kafka.
public class CSVToKafka {
    private static final String TOPIC_NAME = "test-data";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String FILE_PATH = "/home/chriscao/IdeaProjects/data/gps_info.csv";

//    int batchSize = 2;
//    int delayMs = 1000;

    public static void main(String[] args) {
        // 读取CSV文件内容
        List<String[]> csvData = readCSVFile(FILE_PATH);

        // 将CSV数据写入Kafka
        writeToKafka(csvData);
    }

    private static List<String[]> readCSVFile(String filePath) {
        List<String[]> csvData = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                csvData.add(line);
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }

        return csvData;
    }

    private static void writeToKafka(List<String[]> csvData) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Scanner scanner = new Scanner(System.in);
        int size, interval = 0, k = 0, upbound;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                System.out.println("Hint(pattern size (interval_line))：");
                String input = scanner.nextLine();
                // 使用空格分隔输入
                String[] words = input.split(" ");
                if (words[0].equals("run")) {
                    if (words.length==2){
                        try{
                            size = Integer.parseInt(words[1]);
                        }catch(NumberFormatException e) {
                            System.out.println("Please enter a number for size.");
                            continue;
                        }
                    } else if (words.length==3) {
                        try{
                            size = Integer.parseInt(words[1]);
                            interval = Integer.parseInt(words[2]);
                        }catch(NumberFormatException e) {
                            System.out.println("Please enter a number for size and interval.");
                            continue;
                        }
                    }else {
                        System.out.println("Please note the hint.");
                        continue;
                    }
                    upbound = Math.min(size + k, csvData.size());
                    for (int i = k; i<upbound; i++){
                        String value = String.join(",", csvData.get(i));
                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, value);
                        producer.send(record);
                        producer.flush();
                        if (words.length==3){
                            try {
                                Thread.sleep(interval);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
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
                    System.out.println("wrong input for pattern");
                }
            }
        }
        scanner.close();
    }
}

