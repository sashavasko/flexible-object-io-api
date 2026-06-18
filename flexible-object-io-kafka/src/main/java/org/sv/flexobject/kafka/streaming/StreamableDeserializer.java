package org.sv.flexobject.kafka.streaming;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.sv.flexobject.Streamable;

import java.util.Map;

public class StreamableDeserializer implements Deserializer<Streamable> {

    Class<? extends Streamable> valueSchema;

    @Override
    public void configure(Map configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Streamable deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }

    @Override
    public Streamable deserialize(String topic, Headers headers, byte[] data) {
        Class<? extends Streamable> valueSchema = this.valueSchema;

        if (headers != null){
            // TODO
            headers.lastHeader("__TypeId__");
        }
        return Deserializer.super.deserialize(topic, headers, data);
    }
}
