package org.myorg.quickstart;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;


public class IpFunctionAggregate implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private final int maxIpCount;

    public IpFunctionAggregate(int maxIpCount) {
        super();
        this.maxIpCount = maxIpCount;
    }

    @Override
    public Tuple2<String, Integer> createAccumulator() {
        return new Tuple2<>("", 0);
    }

    @Override
    public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
        return new Tuple2<>(value.f0, accumulator.f1 + 1);
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
        return accumulator;
    }
    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> acc1, Tuple2<String, Integer> acc2) {
        return new Tuple2<>(acc1.f0 +acc2.f0, acc1.f1+acc2.f1);
    }
}
