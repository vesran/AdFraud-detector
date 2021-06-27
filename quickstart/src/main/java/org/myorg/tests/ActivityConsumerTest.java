package org.myorg.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.myorg.quickstart.ActivityConsumer;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ActivityConsumerTest {

    @Test
    @DisplayName("Calculate timestamp difference")
    void timestampDifference(){
        assertAll(() -> assertEquals(0, ActivityConsumer.computeReactionTime("1624399636000","1624399636000")),
                () -> assertEquals(0.003, ActivityConsumer.computeReactionTime("1624399636000","1624399666000")),
                () -> assertEquals(3, ActivityConsumer.computeReactionTime("1624399636000","1624429636000")));
    }

}