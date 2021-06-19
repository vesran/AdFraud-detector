package org.myorg.quickstart;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FraudDetector extends KeyedProcessFunction<Long, Activity, Alert> {
    public static final int thresholdActivity = 8;
    public static final double thresholdClickPerDisplay = 0.4;

    private transient ValueState<Integer> countClicksState;
    private transient ValueState<Integer> countDisplaysState;
    private transient ValueState<Long> timerState;

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
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
                "timer-state",
                Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);
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

        Integer numberOfClicks = countClicksState.value();
        Integer numberOfDisplays = countDisplaysState.value();

        // Fraud detection rule for alerting
        if (numberOfDisplays> thresholdActivity && (float) numberOfDisplays / numberOfClicks > thresholdClickPerDisplay)
        {
            Alert alert = new Alert();
            alert.setId(activity.getUid());
            collector.collect(alert);
        }

        // Update states with the event received
        if (activity.isClick()) {
            countClicksState.update(countClicksState.value() + 1);
        }
        else {
            countClicksState.update(countDisplaysState.value() + 1);
        }
    }
}
