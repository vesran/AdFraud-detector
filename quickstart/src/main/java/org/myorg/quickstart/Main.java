package org.myorg.quickstart;

public class Main {

    public static void main(String... args) throws Exception {
        String path_storage = "data/logs_100.txt";
        KafkaConsumerExample.logConsumer(100, path_storage);
    }

}
