package org.myorg.quickstart;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("click")
public class Click extends Activity {

    public Click(){}

    public Click(String eventType, String uid, String timestamp, String ip, String impressionId) {
        super(eventType, uid, timestamp, ip, impressionId);
    }
}
