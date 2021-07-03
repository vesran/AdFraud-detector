package org.myorg.quickstart;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class UidClickCountAggregateFunction implements AggregateFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<>("", 0);
    }

    @Override
    public Tuple2<String, Integer> add(Tuple3<String, String, Integer> value, Tuple2<String, Integer> accumulator) {
        if (value.f1.equals("click")){
            return new Tuple2<>(value.f0, accumulator.f1 + 1);
        }
        else {
            return new Tuple2<>(value.f0, accumulator.f1);
        }
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
        String uid = accumulator.f0;
        //uid = uid.substring(1, uid.length()-1);
        return new Tuple2<>(uid, accumulator.f1);
    }

    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> acc1, Tuple2<String, Integer> acc2) {
        return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
    }
}
