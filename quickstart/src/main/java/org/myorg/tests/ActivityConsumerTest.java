package org.myorg.tests;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.myorg.quickstart.Alert;
import org.myorg.quickstart.FraudulentPatterns;
import org.myorg.quickstart.IpFunctionAggregate;
import org.myorg.quickstart.UidAvgReactionTimeProcess;
import org.apache.flink.streaming.util.*;


import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ActivityConsumerTest {
    //private OneInputStreamOperatorTestHarness<Long, Long> testHarness;
    private UidAvgReactionTimeProcess uidFunctionProcess;
    private static int MIN_REACTION_TIME = 3;
    private static int MAX_CLICK_PER_WINDOW = 3;
    public static int MAX_IP_COUNT = 3;

    @Test
    @DisplayName("Calculate timestamp difference")
    void timestampDifference(){
        assertAll(() -> assertEquals(0, UidAvgReactionTimeProcess.computeReactionTime("1624959723","1624959723")),
                () -> assertEquals(1, UidAvgReactionTimeProcess.computeReactionTime("1624959723","1624959724")),
                () -> assertEquals(3, UidAvgReactionTimeProcess.computeReactionTime("1624959723","1624959720")));
    }

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


}