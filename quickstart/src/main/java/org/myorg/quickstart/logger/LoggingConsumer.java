package org.myorg.quickstart.logger;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.time.Duration;
import java.util.*;


public class LoggingConsumer {

    private final static List<String> TOPICS = Arrays.asList("clicks", "displays");
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(TOPICS);
        return consumer;
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        ConsumerRecords<Long, String> consumerRecords;
        int numEmptyRecords = 0;
        int noRecordsCount = 0;

        while (true) {
            consumerRecords  = consumer.poll(100);

            // Exit if got too much empty record
            if (consumerRecords.count() == 0) {
                numEmptyRecords++;
                if (numEmptyRecords > 100) {
                    break;
                } else {
                    System.out.println("Got empty records for the " + numEmptyRecords + "th time");
                }
            }

            noRecordsCount++;
            System.out.println("Num record : " + noRecordsCount);

            for (ConsumerRecord<Long, String> record : consumerRecords) {
                System.out.println(record.value());  // Print messages
            }

            consumer.commitAsync();

        }
        consumer.close();
        System.out.println("DONE");
    }

    static void logConsumer(int numMaxRecords, String pathname) throws InterruptedException, IOException {
        final Consumer<Long, String> consumer = createConsumer();
        ConsumerRecords<Long, String> consumerRecords;
        Duration window = Duration.ofMillis(1000);
        int noRecordsCount = 0;
        File fout = new File(pathname);
        FileOutputStream fos = new FileOutputStream(fout);
        OutputStreamWriter osw = new OutputStreamWriter(fos);

        while (noRecordsCount < numMaxRecords) {
            consumerRecords  = consumer.poll(window);

            noRecordsCount++;
            System.out.println(noRecordsCount);

            for (ConsumerRecord<Long, String> record : consumerRecords) {
                System.out.println(record.value());
                osw.write(record.value() + "\n");

            }

            consumer.commitAsync();
        }
        osw.close();
        consumer.close();
        System.out.println("DONE");
    }

}
