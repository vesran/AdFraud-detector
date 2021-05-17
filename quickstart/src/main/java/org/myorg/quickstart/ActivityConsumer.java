package org.myorg.quickstart;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;


public class ActivityConsumer {

    public static Date convertToDate(String s) {
        long unix_seconds = Long.parseLong(s);
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
        DataStream<Activity> stream = env.addSource(new FlinkKafkaConsumer<>(topics, new ActivityDeserializationSchema(), properties));

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
                        Map<String, Object> json = new HashMap<>();
                        json.put("timestamp", convertToDate(element.timestamp));
                        json.put("eventType", element.eventType);
                        json.put("uid", element.uid);
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

        stream.addSink(esSinkBuilder.build());

        stream.print();

        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
