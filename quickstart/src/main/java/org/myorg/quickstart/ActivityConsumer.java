package org.myorg.quickstart;

import java.sql.Timestamp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import scala.Int;

import java.time.Duration;
import java.util.*;


public class ActivityConsumer {

    public static int MAX_CLICKS_PER_WINDOW = 10;
    public static int MIN_REACTION_TIME = 3;

    public static void main(String[] args) {

        List<String> topics = new ArrayList<>();
        topics.add("clicks");
        topics.add("displays");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer<Activity> kafkaSource = new FlinkKafkaConsumer<>(topics, new ActivityDeserializationSchema(), properties);
        kafkaSource.assignTimestampsAndWatermarks(
                WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(1)));
        DataStream<Activity> stream = env.addSource(kafkaSource);

        DataStream<Tuple3<String, String, String>> uidAnalysis = stream
                .map((MapFunction<Activity, Tuple3<String, String, String>>) activity -> new Tuple3<>(activity.getUid(), activity.getTimestamp(), activity.getEventType()))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .process(new ProcessWindowFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple, TimeWindow>() {
                            @Override
                            public void process(Tuple key, Context context, Iterable<Tuple3<String, String, String>> iterable, Collector<Tuple3<String, String, String>> collector) {
                                int count = 0;
                                Tuple3<String, String, String> previous = null;
                                for (Tuple3<String, String, String> in : iterable) {
                                    if (in.f2.equals("click")) {
                                        count++;
                                    }
                                    if (previous != null && previous.f2.equals("display") && in.f2.equals("click") && computeReactionTime(previous.f1, in.f1) < MIN_REACTION_TIME) {
                                        Alert alert = new Alert(FraudulentPatterns.LOW_REACTION_TIME);
                                        collector.collect(new Tuple3<>(key.toString(), alert.toString(), context.window().toString()));
                                    }
                                    previous = in;
                                }
                                if (count > MAX_CLICKS_PER_WINDOW) {
                                    Alert alert = new Alert(FraudulentPatterns.MANY_CLICKS);
                                    collector.collect(new Tuple3<>(key.toString(), alert.toString(), context.window().toString()));
                                }
                            }
                        });

        uidAnalysis.print();

        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param timestamp1 first timestamp
     * @param timestamp2 second timestamp
     * @return time difference in seconds
     */
    public static double computeReactionTime(String timestamp1, String timestamp2) {
        Long timestamp_1 = Long.parseLong(timestamp1);
        Long timestamp_2 = Long.parseLong(timestamp2);
        // time difference in seconds
        return Math.abs(timestamp_2 - timestamp_1);
    }
}
