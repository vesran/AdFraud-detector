package org.myorg.quickstart;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
        property = "eventType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Display.class, name = "display"),
        @JsonSubTypes.Type(value = Click.class, name = "click")
    }
)
public abstract class Activity {

    @JsonProperty("uid")            String uid;
    @JsonProperty("timestamp")      String timestamp;
    @JsonProperty("ip")             String ip;
    @JsonProperty("impressionId")   String impressionId;

    public Date getTimestampAsDate() {
        long unix_seconds = Long.parseLong(this.timestamp);
        return new Date(unix_seconds * 1000L);
    }

}
