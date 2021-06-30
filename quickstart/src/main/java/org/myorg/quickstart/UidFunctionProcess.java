package org.myorg.quickstart;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


public class UidFunctionProcess extends ProcessWindowFunction<Tuple5<String, String, String, String, String>, Alert, Tuple, TimeWindow>{

    private int minReactionTime;
    private int maxClickPerWindow;

    public UidFunctionProcess(int minReactionTime, int maxClickPerWindow) {
        super();
        this.minReactionTime = minReactionTime;
        this.maxClickPerWindow = maxClickPerWindow;
    }

    @Override
    public void process(Tuple key, Context context, Iterable<Tuple5<String, String, String, String, String>> iterable, Collector<Alert> collector) throws Exception  {
        int count = 0;
        double average_reaction_time = 0;
        double acc_size = 0;
        ArrayList<Tuple5<String, String, String, String, String>> previouses = new ArrayList<>();
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
                if (average_reaction_time<this.minReactionTime && acc_size>0)
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
        if (count > this.maxClickPerWindow) {
            Alert alert = new Alert(FraudulentPatterns.MANY_CLICKS);
            alert.setId(key.toString());
            collector.collect(alert);
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
