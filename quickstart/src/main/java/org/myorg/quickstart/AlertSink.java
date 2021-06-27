package org.myorg.quickstart;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class AlertSink extends Object
        implements SinkFunction<Alert> {
    public AlertSink(){
    }
    public void invoke(Alert value,
                       SinkFunction.Context context){
        System.out.println("Potential fraudulent action detected for user with uid : "+value.getId()+" with "+value.getAlertPattern());
    }
}
