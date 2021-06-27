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


        DataStream<Activity> stream = env.addSource(new FlinkKafkaConsumer<>(topics, new ActivityDeserializationSchema(), properties))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(1)));


        DataStream<Alert> alerts;
        alerts = stream
                .keyBy(Activity::getUid)
                .process(new KeyedProcessFunction<String, Activity, Alert>() { // Anonymous class because there is for some reason a problem when passing fraud detector
                    public static final int thresholdActivity = 8;
                    public static final double thresholdClickPerDisplay = 0.75;

                    private transient ValueState<Integer> countClicksState;
                    private transient ValueState<Integer> countDisplaysState;
                    //private transient ValueState<Long> timerState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Integer> flagClickDescriptor = new ValueStateDescriptor<>(
                                "countClicks",
                                Types.INT);
                        countClicksState = getRuntimeContext().getState(flagClickDescriptor);
                        ValueStateDescriptor<Integer> flagDisplayDescriptor = new ValueStateDescriptor<>(
                                "countDisplays",
                                Types.INT);
                        countDisplaysState = getRuntimeContext().getState(flagDisplayDescriptor);
                       /* ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                                "timer-state",
                                Types.LONG);
                        timerState = getRuntimeContext().getState(timerDescriptor);*/
                    }

                    @Override
                    public void processElement(Activity activity, Context context, Collector<Alert> collector) throws Exception {

                        // Set the initial state for counting purpose
                        if (countClicksState.value() == null) {
                            countClicksState.update(0);
                        }
                        if (countDisplaysState.value() == null) {
                            countDisplaysState.update(0);
                        }

                        // Update states with the event received
                        if (activity.isClick()) {
                            countClicksState.update(countClicksState.value() + 1);
                        }
                        else {
                            countDisplaysState.update(countDisplaysState.value() + 1);
                        }

                        Integer numberOfClicks = countClicksState.value();
                        Integer numberOfDisplays = countDisplaysState.value();

                        System.out.println("Number of displays for uid : "+activity.getUid()+" is : "+numberOfDisplays+" and number of clics is "+numberOfClicks);
                        // Fraud detection rule for alerting
                        if (numberOfDisplays> thresholdActivity && (float) numberOfClicks / numberOfDisplays > thresholdClickPerDisplay)
                        {
                            Alert alert = new Alert(FraudulentPatterns.MANY_CLICKS);
                            alert.setUidClickPerDisplayRatio((float) numberOfClicks / numberOfDisplays);
                            alert.setId(activity.getUid());
                            collector.collect(alert);
                        }
                    }
                })
                .name("fraud-detector");

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Activity> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Activity>() {
                    @Override
                    public void process(Activity activity, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(activity));
                    }

                    public IndexRequest createIndexRequest(Activity element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("eventType", element.eventType);
                        json.put("uid", element.uid);
                        json.put("timestamp", element.timestamp);
                        json.put("ip", element.ip);
                        json.put("impressionId", element.impressionId);

                        return Requests.indexRequest()
                                .index("clicksdisplaysidx")
                                .type("Activity")
                                .source(json);
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);
        stream.
                 addSink(esSinkBuilder.build());
        alerts
                .addSink(new AlertSink())
                .name("send-alerts");

        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
