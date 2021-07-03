package org.myorg.tests;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.elasticsearch.index.mapper.Uid;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.myorg.quickstart.*;
//import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness; //Cannot be imported :(


import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ActivityConsumerTest {
   // private OneInputStreamOperatorTestHarness<Long, Long> testHarness;
    private UidAvgReactionTimeProcess uidFunctionProcess;
    private static int MIN_REACTION_TIME = 3;
    private static int MAX_CLICK_PER_WINDOW = 3;
    public static int MAX_IP_COUNT = 3;
    public static int WINDOW_SIZE = 10;
    public static Time TIME_WINDOW = Time.minutes(WINDOW_SIZE);


    @Test
    public void testIpAggregate() throws Exception {
        // instantiate your function
        IpFunctionAggregate aggregator = new IpFunctionAggregate(MAX_IP_COUNT);

        // call the methods that you have implemented
        assertEquals(new Tuple2<String, Integer>("testIP",3), aggregator.add(new Tuple2<String, Integer>("testIP",0), new Tuple2<String, Integer>("",2)));
        assertEquals(new Tuple2<String, Integer>("testIP",3), aggregator.getResult(new Tuple2<String, Integer>("testIP",3)));
    }

    @Test
    public void UidAvgReactionTimeAggregateFunction() throws Exception {
        // instantiate your function
        IpFunctionAggregate aggregator = new IpFunctionAggregate(MAX_IP_COUNT);

        // call the methods that you have implemented
        assertEquals(new Tuple2<String, Integer>("testIP",3), aggregator.add(new Tuple2<String, Integer>("testIP",0), new Tuple2<String, Integer>("",2)));
        assertEquals(new Tuple2<String, Integer>("testIP",3), aggregator.getResult(new Tuple2<String, Integer>("testIP",3)));
    }

    @Test
    public void UidAvgReactionTimeProcess() throws Exception {
        ArrayList<String> testResult = new ArrayList<>();
        List<String> expectedResult = Arrays.asList("(uid1,1.3333333333333333)");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<Tuple5<String, String, String, String, String>> stream = env.addSource(new SourceFunction<Tuple5<String, String, String, String, String>>()
        {
            @Override
            public void run(SourceContext<Tuple5<String, String, String, String, String>> ctx) throws Exception {
                ctx.collect(Tuple5.of("uid1","1624959721","display","ip1","impressionId1"));
                ctx.collect(Tuple5.of("uid1","1624959722","click","ip1","impressionId1"));
                ctx.collect(Tuple5.of("uid1","1624959723","display","ip1","impressionId1"));
                ctx.collect(Tuple5.of("uid1","1624959724","display","ip2","impressionId2"));
                ctx.collect(Tuple5.of("uid1","1624959725","display","ip1","impressionId1"));
                ctx.collect(Tuple5.of("uid1","1624959726","click","ip2","impressionId2"));
                ctx.collect(Tuple5.of("uid1","1624959727","display","ip3","impressionId3"));
                ctx.collect(Tuple5.of("uid1","1624959728","click","ip3","impressionId3"));
                ctx.collect(Tuple5.of("uid1","1624959729","display","ip1","impressionId1"));
                // source is finite, so it will have an implicit MAX watermark when it finishes
            }
            @Override
            public void cancel() {
            }
        }).assignTimestampsAndWatermarks((new UtilTuple5TimestampExtractorForTest()));

        SingleOutputStreamOperator test = stream.keyBy(0).window(TumblingEventTimeWindows.of(TIME_WINDOW)).process(new UidAvgReactionTimeProcess());
        test.addSink(new SinkFunction() {
            @Override
            public void invoke(Object value, Context context) throws Exception {
                testResult.add(value.toString());
                assertEquals(expectedResult, testResult);
            }

        });
        env.execute("Uid Avg Reaction time process window test");
    }

    @Test
    @DisplayName("Calculate timestamp difference")
    void timestampDifference(){
        assertAll(() -> assertEquals(0, UidAvgReactionTimeProcess.computeReactionTime("1624959723","1624959723")),
                () -> assertEquals(1, UidAvgReactionTimeProcess.computeReactionTime("1624959723","1624959724")),
                () -> assertEquals(3, UidAvgReactionTimeProcess.computeReactionTime("1624959723","1624959720")));
    }

}
