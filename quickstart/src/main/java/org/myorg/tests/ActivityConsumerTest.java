package org.myorg.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.myorg.quickstart.ActivityConsumer;
import org.myorg.quickstart.UidFunctionProcess;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ActivityConsumerTest {

    @Test
    @DisplayName("Calculate timestamp difference")
    void timestampDifference(){
        assertAll(() -> assertEquals(0, UidFunctionProcess.computeReactionTime("1624959723","1624959723")),
                () -> assertEquals(1, UidFunctionProcess.computeReactionTime("1624959723","1624959724")),
                () -> assertEquals(3, UidFunctionProcess.computeReactionTime("1624959723","1624959720")));
    }

}