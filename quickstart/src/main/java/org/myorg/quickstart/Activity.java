package org.myorg.quickstart;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;


public class Activity {

    @JsonProperty("eventType")   String eventType;
    @JsonProperty("uid")   String uid;
    @JsonProperty("timestamp")   String timestamp;
    @JsonProperty("ip")   String ip;
    @JsonProperty("impressionId")   String impressionId;

    public Activity(){}

    public Activity(String eventType, String uid, String timestamp, String ip, String impressionId)
    {
        this.eventType = eventType;
        this.uid = uid;
        this.timestamp = timestamp;
        this.ip = ip;
        this.impressionId = impressionId;
    }

    public String getUid() {
        return this.uid;
    }

    public String getIp() { return this.ip; }

    @Override
    public String toString() {
        StringBuilder strb = new StringBuilder();
        strb.append(this.eventType);
        strb.append(" -> ");
        strb.append(" uid : ");
        strb.append(this.uid);
        strb.append(", timestamp : ");
        strb.append(this.timestamp);
        strb.append(", ip : ");
        strb.append(this.ip);
        strb.append(", impressionId : ");
        strb.append(this.impressionId);

        return strb.toString();
    }
}

/*


public class Activity {
    //using java.util.Date for better readability
    //@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss:SSS")
    private String eventType;
    private String uid;
    private String timestamp;
    private String ip;
    private String impressionId;

    public Activity(final String eventType, final String uid, final String timestamp, final String ip, final String impressionId)
    {
        this.eventType = eventType;
        this.uid = uid;
        this.timestamp = timestamp;
        this.ip = ip;
        this.impressionId = impressionId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("eventType").append(eventType);
        sb.append("uid").append(uid);
        sb.append("timestamp").append(timestamp);
        sb.append("ip").append(ip);
        sb.append("impressionId").append(impressionId).append("}");
        return sb.toString();
    }
}
 */
