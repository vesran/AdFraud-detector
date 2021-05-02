package org.myorg.quickstart;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ActivityDeserializationSchema implements DeserializationSchema<Activity> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Activity deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Activity.class);
    }

    @Override
    public boolean isEndOfStream(Activity nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Activity> getProducedType() {
        return TypeInformation.of(Activity.class);

    }
}
