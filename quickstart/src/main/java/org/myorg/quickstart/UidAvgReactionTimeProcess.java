package org.myorg.quickstart;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class UidAvgReactionTimeProcess extends ProcessWindowFunction<Tuple5<String, String, String, String, String>, Tuple2<String, Double>, Tuple, TimeWindow> {

    public UidAvgReactionTimeProcess() {
        super();
    }

    @Override
    public void process(Tuple key, Context context, Iterable<Tuple5<String, String, String, String, String>> iterable, Collector<Tuple2<String, Double>> collector) throws Exception {
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
                            break;
                        }
                    }
                }
            }
            previouses.add(in);
        }

        String uid = key.toString();
        uid = uid.substring(1, uid.length()-1);
        if (acc_size > 0) {
            average_reaction_time /= acc_size;
            collector.collect(new Tuple2<>(uid, average_reaction_time));
        } else {
            // If no click, set reaction time to positive inf to filter them easily
            // since we only want reaction time below a threshold.
            collector.collect(new Tuple2<>(uid, 10000.));
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
