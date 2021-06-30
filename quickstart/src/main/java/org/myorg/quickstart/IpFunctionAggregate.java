package org.myorg.quickstart;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class IpFunctionAggregate implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Alert> {

    private int maxIpCount;

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
        //return new Tuple2<>(value.f0, value.f1+accumulator.f1);
        return new Tuple2<>(value.f0, 1+accumulator.f1);
    }

    @Override
    public Alert getResult(Tuple2<String, Integer> accumulator) {
        //System.out.println(accumulator);
        if (accumulator.f1>= this.maxIpCount)
        {
            Alert alert = new Alert(FraudulentPatterns.MANY_EVENTS_FOR_IP);
            alert.setIp(accumulator.f0);
            return alert;
        }
        else
            return null;
    }
    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> acc1, Tuple2<String, Integer> acc2) {
        return new Tuple2<>(acc1.f0 +acc2.f0, acc1.f1+acc2.f1);
    }
}
