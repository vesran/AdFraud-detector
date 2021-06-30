package org.myorg.quickstart.logger;

import org.myorg.quickstart.logger.LoggingConsumer;

public class Main {

    public static void main(String... args) throws Exception {
        String path_storage = "data/logs_100.txt";
        LoggingConsumer.logConsumer(100, path_storage);
    }

}
