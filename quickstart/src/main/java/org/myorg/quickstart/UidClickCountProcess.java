package org.myorg.quickstart;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class UidClickCountProcess extends ProcessWindowFunction<Tuple5<String, String, String, String, String>, Tuple2<String, Integer>, Tuple, TimeWindow> {
    @Override
    public void process(Tuple key, Context context, Iterable<Tuple5<String, String, String, String, String>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception  {
        int count = 0;
        for (Tuple5<String, String, String, String, String> in : iterable) {
            if (in.f2.equals("click")) {
                count++;
            }
        }
        String uid = key.toString();
        uid = uid.substring(1, uid.length()-1);
        collector.collect(new Tuple2<>(uid, count));
    }
}
