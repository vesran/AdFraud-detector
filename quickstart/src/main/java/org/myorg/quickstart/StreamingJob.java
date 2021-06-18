package org.myorg.quickstart;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;

import java.time.Duration;
import java.util.*;


public class StreamingJob {

    public static int MAX_IP_COUNT = 10;

    public static Date convertToDate(String s) {
        Long unix_seconds = Long.parseLong(s);
        return new Date(unix_seconds * 1000L);
    }

    public static void main(String[] args) {
        List<String> topics = new ArrayList<>();
        topics.add("clicks");
        topics.add("displays");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        DataStream<Activity> stream = env.addSource(new FlinkKafkaConsumer<>(topics, new ActivityDeserializationSchema(), properties))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)));


        DataStream<Tuple2<String, Integer>> ipCount = stream
                .map(new MapFunction<Activity, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Activity activity) throws Exception {
                        return new Tuple2<>(activity.getIp(), 1);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(1);

        // Output ip addresses which look suspicious
        ipCount.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.f1 >= MAX_IP_COUNT;
            }
        }).print();//.writeAsText("data/out.txt").setParallelism(1);


        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
