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

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getUid() {
        return this.uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getImpressionId() {
        return impressionId;
    }

    public void setImpressionId(String impressionId) {
        this.impressionId = impressionId;
    }

    public boolean isClick(){
        return this.eventType.equals("click");
    }

    public boolean isDisplay(){
        return this.eventType.equals("display");
    }

}

