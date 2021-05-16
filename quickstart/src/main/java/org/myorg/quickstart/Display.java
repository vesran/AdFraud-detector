package org.myorg.quickstart;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("display")
public class Display extends Activity{

    public Display(){}

    public Display(String eventType, String uid, String timestamp, String ip, String impressionId) {
        super(eventType, uid, timestamp, ip, impressionId);
    }
}
