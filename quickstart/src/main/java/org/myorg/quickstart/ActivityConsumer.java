package org.myorg.quickstart;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ActivityConsumer {


    public static void main(String[] args) {
        List<String> topics = new ArrayList<>();
        topics.add("clicks");
        topics.add("displays");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        DataStream<Activity> stream = env.addSource(new FlinkKafkaConsumer<>(topics, new ActivityDeserializationSchema(), properties));


        stream.print();

        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
