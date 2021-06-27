package org.myorg.quickstart;

import java.sql.Timestamp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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
                .forBoundedOutOfOrderness(Duration.ofSeconds(10)));
        DataStream<Activity> stream = env.addSource(kafkaSource);

        DataStream<Tuple2<String, Integer>> uidAnalysis = stream
                .keyBy(Activity::getUid)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(new ProcessWindowFunction<Activity, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Activity> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Integer count = 0;
                        Activity previous = null;
                        System.out.println("key of key by : "+key+" and key from iterable : "+key);
                        for (Activity in: iterable) {
                            if(in.isClick()){
                                count++;
                            }
                            if (previous != null) {
                                System.out.println(" previous "+previous.timestamp+" in "+in.timestamp+" time diff in seconds "+computeReactionTime(previous.timestamp, in.timestamp));
                            }
                            if (previous != null && previous.isDisplay() && in.isClick() && computeReactionTime(previous.timestamp, in.timestamp) < MIN_REACTION_TIME) {
                                System.out.println("Alert for user : "+key+" with a reaction time too fast (<1 seconds)");
                            }
                            previous = in;
                        }
                        if (count > MAX_CLICKS_PER_WINDOW)
                        {
                            System.out.println("Alert for user : "+previous.uid+" with a number of clicks per window too high");
                        }
                        System.out.println("Window: "+context.window().toString()+" watermark: "+context.currentWatermark());
                        collector.collect(new Tuple2<String, Integer>(context.window().toString(), count));
                    }
                });

        /*uidAnalysis
                .addSink(new AlertSink())
                .name("send-alerts");*/

        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param timestamp1
     * @param timestamp2
     * @return time difference in seconds
     */
    public static double computeReactionTime(String timestamp1, String timestamp2) {
        Timestamp timestamp_1 = new Timestamp(Long.parseLong(timestamp1));
        Timestamp timestamp_2 = new Timestamp(Long.parseLong(timestamp2));
        // time difference in seconds
        long milliseconds = Math.abs(timestamp_2.getTime() - timestamp_1.getTime());
        double seconds = (double) milliseconds / 10000000;
        return seconds;
    }
}
