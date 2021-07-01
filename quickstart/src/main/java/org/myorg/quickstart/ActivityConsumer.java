package org.myorg.quickstart;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

import java.time.Duration;
import java.util.*;


public class ActivityConsumer {

    public static int MAX_CLICKS_PER_WINDOW = 15;
    public static int MIN_REACTION_TIME = 3;
    public static int MAX_IP_COUNT = 10;
    public static int WINDOW_SIZE = 10;
    public static Time TIME_WINDOW = Time.minutes(WINDOW_SIZE);

    public static Date convertToDate(String s) {
        long unix_seconds = Long.parseLong(s);
        return new Date(unix_seconds * 1000L);
    }

    public static ElasticsearchSink<ActivityStat> getESSink(String indexName) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<ActivityStat> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<ActivityStat>() {
                    @Override
                    public void process(ActivityStat activity, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(activity));
                    }

                    public IndexRequest createIndexRequest(ActivityStat element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("timestamp", convertToDate(element.getTimestamp()));
                        json.put("eventType", element.getEventType());
                        json.put("uid", element.getUid());
                        json.put("ip", element.getIp());
                        json.put("impressionId", element.getImpressionId());
                        json.put("numIpByUid", element.getNumIpByUid());
                        json.put("reactionTime", element.getReactionTime());
                        json.put("numClicks", element.getNumClicks());

                        return Requests.indexRequest()
                                .index(indexName)
                                .type("ActivityStat")
                                .source(json);
                    }
                }
        );
        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);
        return esSinkBuilder.build();
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
                .window(TumblingEventTimeWindows.of(TIME_WINDOW))
                .aggregate(new IpFunctionAggregate(MAX_IP_COUNT));

        // Output ip alert
        DataStream<Alert> ipAlerts = ipCount
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

        WindowedStream<Tuple5<String, String, String, String, String>, Tuple, TimeWindow> uidStream = stream
                .map(new MapFunction<Activity, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple5<String, String, String, String, String> map(Activity activity) throws Exception {
                        return new Tuple5<>(activity.getUid(), activity.getTimestamp(), activity.getEventType(), activity.getImpressionId(), activity.getIp());
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(TIME_WINDOW));

        DataStream<Tuple2<String, Double>> uidReactionTimes = uidStream
            .process(new UidAvgReactionTimeProcess());
        DataStream<Alert> uidAlertReactionTime = uidReactionTimes
                .filter(new FilterFunction<Tuple2<String, Double>>() {
                    @Override
                    public boolean filter(Tuple2<String, Double> tuple) throws Exception {
                        return tuple.f1 < MIN_REACTION_TIME;
                    }
                })
                .map(new MapFunction<Tuple2<String, Double>, Alert>() {
                    @Override
                    public Alert map(Tuple2<String, Double> tuple) throws Exception {
                        Alert alert = new Alert(FraudulentPatterns.LOW_REACTION_TIME);
                        alert.setId(tuple.f0);
                        alert.setTimeReaction(tuple.f1);
                        return alert;
                    }
                });


        DataStream<Tuple2<String, Integer>> uidClickCounts = uidStream
                .process(new UidClickCountProcess());
        DataStream<Alert> uidAlertClicks = uidClickCounts
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple.f1 > MAX_CLICKS_PER_WINDOW;
                    }
                })
                .map(new MapFunction<Tuple2<String, Integer>, Alert>() {
                    @Override
                    public Alert map(Tuple2<String, Integer> tuple) throws Exception {
                        Alert alert = new Alert(FraudulentPatterns.MANY_CLICKS);
                        alert.setId(tuple.f0);
                        alert.setNumClick(tuple.f1);
                        return alert;
                    }
                });

        // Join activity stream with ipCount : having the number of event associated to an ip in an activity
        DataStream<ActivityStat> activityStats = stream
                .join(ipCount)
                .where(Activity::getIp)
                .equalTo(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(TIME_WINDOW))
                .apply(new JoinFunction<Activity, Tuple2<String, Integer>, ActivityStat>() {
                    @Override
                    public ActivityStat join(Activity activity, Tuple2<String, Integer> numIp) throws Exception {
                        ActivityStat a = new ActivityStat(activity.getEventType(), activity.getUid(),
                                activity.getTimestamp(), activity.getIp(), activity.getImpressionId());
                        a.setNumIpByUid(numIp.f1);
                        return a;
                    }
                })
                .join(uidReactionTimes)
                .where(ActivityStat::getUid)
                .equalTo(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(TIME_WINDOW))
                .apply(new JoinFunction<ActivityStat, Tuple2<String, Double>, ActivityStat>() {
                    @Override
                    public ActivityStat join(ActivityStat activityStat, Tuple2<String, Double> tuple) throws Exception {
                        ActivityStat a = new ActivityStat(activityStat.getEventType(), activityStat.getUid(),
                                activityStat.getTimestamp(), activityStat.getIp(), activityStat.getImpressionId());
                        a.setNumIpByUid(activityStat.getNumIpByUid());
                        a.setReactionTime(tuple.f1);
                        return a;
                    }
                })
                .join(uidClickCounts)
                .where(ActivityStat::getUid)
                .equalTo(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(TIME_WINDOW))
                .apply(new JoinFunction<ActivityStat, Tuple2<String, Integer>, ActivityStat>() {
                    @Override
                    public ActivityStat join(ActivityStat activityStat, Tuple2<String, Integer> tuple) throws Exception {
                        ActivityStat a = new ActivityStat(activityStat.getEventType(), activityStat.getUid(),
                                activityStat.getTimestamp(), activityStat.getIp(), activityStat.getImpressionId());
                        a.setNumIpByUid(activityStat.getNumIpByUid());
                        a.setReactionTime(activityStat.getReactionTime());
                        a.setNumClicks(tuple.f1);
                        return a;
                    }
                })
                ;


        activityStats.addSink(getESSink("activitystats"));

        uidAlertReactionTime
                .addSink(new AlertSink())
                .name("Uid Time Reaction Alert Sink");

        uidAlertClicks
                .addSink(new AlertSink())
                .name("Uid Number of Click Alert Sink");

        ipAlerts
                .addSink(new AlertSink())
                .name("IP Alert Sink");

        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

