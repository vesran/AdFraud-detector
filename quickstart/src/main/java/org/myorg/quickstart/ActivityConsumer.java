package org.myorg.quickstart;

import java.sql.Timestamp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
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
import scala.Array;
import scala.Int;

import java.time.Duration;
import java.util.*;


public class ActivityConsumer {

    public static int MAX_CLICKS_PER_WINDOW = 10;
    public static int MIN_REACTION_TIME = 3;
    public static int MAX_IP_COUNT = 10;

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

        DataStream<Alert> ipCount = stream
                .map(new MapFunction<Activity,Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Activity activity) throws Exception {
                        return new Tuple2<>(activity.getIp(), 1);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Alert>() {
                               @Override
                               public Tuple2<String, Integer> createAccumulator() {
                                   return new Tuple2<>("", 0);
                               }

                               @Override
                               public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
                                   //return new Tuple2<>(value.f0, value.f1+accumulator.f1);
                                   return new Tuple2<>(value.f0, 1+accumulator.f1);
                               }

                               @Override
                               public Alert getResult(Tuple2<String, Integer> accumulator) {
                                   System.out.println(accumulator);
                                   if (accumulator.f1>= MAX_IP_COUNT)
                                   {
                                       Alert alert = new Alert(FraudulentPatterns.MANY_EVENTS_FOR_IP);
                                       alert.setIp(accumulator.f0);
                                       return alert;
                                   }
                                   else
                                       return null;
                               }
                               @Override
                               public Tuple2<String, Integer> merge(Tuple2<String, Integer> acc1, Tuple2<String, Integer> acc2) {
                                   return new Tuple2<>(acc1.f0 +acc2.f0, acc1.f1+acc2.f1);
                               }
                           }
                );

        DataStream<Alert> uidAlerts = stream
                .map(new MapFunction<Activity, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple5<String, String, String, String, String> map(Activity activity) throws Exception {
                        return new Tuple5<>(activity.getUid(), activity.getTimestamp(), activity.getEventType(), activity.getImpressionId(), activity.getIp());
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple5<String, String, String, String, String>, Alert, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple key, Context context, Iterable<Tuple5<String, String, String, String, String>> iterable, Collector<Alert> collector) {
                        int count = 0;
                        double average_reaction_time = 0;
                        double acc_size = 0;
                        ArrayList<Tuple5<String, String, String, String, String>> previouses = new ArrayList();
                        for (Tuple5<String, String, String, String, String> in : iterable) {
                            if (in.f2.equals("click")) {
                                count++;
                                if (!previouses.isEmpty()) {
                                    for (Tuple5<String, String, String, String, String>  previous_event : previouses) {
                                        if (previous_event.f2.equals("display") && previous_event.f3.equals(in.f3) && previous_event.f4.equals((in.f4))){
                                            average_reaction_time+=computeReactionTime(previous_event.f1,in.f1);
                                            acc_size++;
                                        }
                                    }
                                }
                                if (acc_size > 0) {
                                    average_reaction_time /= acc_size;
                                }
                                if (average_reaction_time<MIN_REACTION_TIME && acc_size>0)
                                {
                                    Alert alert = new Alert(FraudulentPatterns.LOW_REACTION_TIME);
                                    alert.setId(key.toString());
                                    alert.setImpressionId(in.f3);
                                    alert.setIp(in.f4);
                                    collector.collect(alert);
                                }
                            }
                            previouses.add(in);
                        }
                        if (count > MAX_CLICKS_PER_WINDOW) {
                            Alert alert = new Alert(FraudulentPatterns.MANY_CLICKS);
                            alert.setId(key.toString());
                            collector.collect(alert);
                        }
                    }
                });

        uidAlerts
                .addSink(new AlertSink())
                .name("Uid Alert Sink");

        ipCount
                .addSink(new AlertSink())
                .name("IP Alert Sink");

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
    public static double computeReactionTime (String timestamp1, String timestamp2){
        Long timestamp_1 = Long.parseLong(timestamp1);
        Long timestamp_2 = Long.parseLong(timestamp2);
        // time difference in seconds
        return Math.abs(timestamp_2 - timestamp_1);
    }
}

