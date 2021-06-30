package org.myorg.quickstart;

import java.sql.Timestamp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
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
    public static int WINDOW_SIZE = 10;

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

        DataStream<Tuple2<String, Integer>> ipCount = stream
                .map(new MapFunction<Activity,Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Activity activity) throws Exception {
                        return new Tuple2<>(activity.getIp(), 1);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE)))
                .aggregate(new IpFunctionAggregate(MAX_IP_COUNT));

        // Join activity stream with ipCount : having the number of event associated to an ip in an activity
        DataStream<ActivityStat> coStream = stream
                .join(ipCount)
                .where(Activity::getIp)
                .equalTo(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE)))
                .apply(new JoinFunction<Activity, Tuple2<String, Integer>, ActivityStat>() {
                    @Override
                    public ActivityStat join(Activity activity, Tuple2<String, Integer> numIp) throws Exception {
                        return new ActivityStat(activity.getEventType(), activity.getUid(),
                                activity.getTimestamp(), activity.getIp(), activity.getImpressionId(),
                                numIp.f1);
                    }
                });
        coStream.print();

        // Output ip alert
        DataStream<Alert> ipAlertStream = ipCount
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.f1 >= MAX_IP_COUNT;
                    }
                })
                .map(new MapFunction<Tuple2<String, Integer>, Alert>() {
                    @Override
                    public Alert map(Tuple2<String, Integer> tuple) throws Exception {
                        Alert alert = new Alert(FraudulentPatterns.MANY_EVENTS_FOR_IP);
                        alert.setIp(tuple.f0);
                        return alert;
                    }
                });

        DataStream<Alert> uidAlerts = stream
                .map(new MapFunction<Activity, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple5<String, String, String, String, String> map(Activity activity) throws Exception {
                        return new Tuple5<>(activity.getUid(), activity.getTimestamp(), activity.getEventType(), activity.getImpressionId(), activity.getIp());
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE)))
                .process(new UidFunctionProcess(MIN_REACTION_TIME, MAX_CLICKS_PER_WINDOW));



//        uidAlerts
//                .addSink(new AlertSink())
//                .name("Uid Alert Sink");
//
//        ipAlertStream
//                .addSink(new AlertSink())
//                .name("IP Alert Sink");



        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

