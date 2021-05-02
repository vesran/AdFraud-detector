package org.myorg.quickstart;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
        property = "eventType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Display.class, name = "display"),
        @JsonSubTypes.Type(value = Click.class, name = "click")
    }
)
public abstract class Activity {

    @JsonProperty("eventType")      String eventType;
    @JsonProperty("uid")            String uid;
    @JsonProperty("timestamp")      String timestamp;
    @JsonProperty("ip")             String ip;
    @JsonProperty("impressionId")   String impressionId;


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
