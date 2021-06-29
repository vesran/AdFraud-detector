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
                .map(new MapFunction<Activity, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(Activity activity) throws Exception {
                        return new Tuple3<>(activity.getUid(), activity.getTimestamp(), activity.getEventType());
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .aggregate(new AggregateFunction<Tuple3<String, String, String>, Object, Tuple3<String, String, String>>() {
                               @Override
                               public Object createAccumulator() {
                                   return new Integer(0);
                               }

                               @Override
                               public Object add(Tuple3<String, String, String> stringStringStringTuple3, Object o) {
                                   return null;
                               }

                               @Override
                               public Tuple3<String, String, String> getResult(Object o) {
                                   return null;
                               }

                               @Override
                               public Object merge(Object o, Object acc1) {
                                   return null;
                               }
                           }
                        , new ProcessWindowFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple, TimeWindow>() {
                            @Override
                            public void process(Tuple key, Context context, Iterable<Tuple3<String, String, String>> iterable, Collector<Tuple3<String, String, String>> collector) throws Exception {
                                Integer count = 0;
                                Tuple3<String, String, String> previous = null;
                                System.out.println(" Start of Window: " + context.window().toString() + " watermark: " + context.currentWatermark() + " with key : " + key + "and key in iteratror " + iterable.iterator().next().f0);
                                //System.out.println("key of key by "+key);
                                //System.out.println("let's see the event for this key in the window");
                                for (Tuple3<String, String, String> in : iterable) {
                                    System.out.println("key of iterable " + in.f0);
                                    if (in.f2.equals("click")) {
                                        count++;
                                    }
                                    if (previous != null) {
                                        System.out.println("diff in compute time is " + computeReactionTime(previous.f1, in.f1) + "for time1 : " + previous.f1 + " with uid : " + previous.f0 + " and time2 : " + in.f1 + " with uid : " + in.f0);
                                    }
                                    if (previous != null && previous.f2.equals("display") && in.f2.equals("click") && computeReactionTime(previous.f1, in.f1) < MIN_REACTION_TIME) {
                                        System.out.println("Alert for user : " + "key.getField(0)" + " with a reaction time too fast (<1 seconds)");
                                    }
                                    previous = in;
                                }
                                if (count > MAX_CLICKS_PER_WINDOW) {
                                    System.out.println("Alert for user : " + "key.getField(0)" + " with a number of clicks per window too high");
                                }
                                //System.out.println("End of Window: "+context.window().toString()+" watermark: "+context.currentWatermark());
                                collector.collect(new Tuple3<String, String, String>("", "", ""));
                            }
                        });


        uidAnalysis.print();

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
        Long timestamp_1 = Long.parseLong(timestamp1);
        Long timestamp_2 = Long.parseLong(timestamp2);
        // time difference in seconds
        long secondsDifference = Math.abs(timestamp_2 - timestamp_1);
        return secondsDifference;
    }
}
