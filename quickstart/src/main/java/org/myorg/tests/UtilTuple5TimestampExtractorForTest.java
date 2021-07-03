package org.myorg.tests;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

class UtilTuple5TimestampExtractorForTest
        implements AssignerWithPunctuatedWatermarks<Tuple5<String, String, String, String ,String>> {

    @Override
    public long extractTimestamp(Tuple5<String, String, String, String, String> element, long previousTimestamp) {
        return new Long(element.f1);
    }

    @Override
    public Watermark checkAndGetNextWatermark(
            Tuple5<String, String, String, String, String> element, long extractedTimestamp) {
        return new Watermark(extractedTimestamp - 1);
    }
}