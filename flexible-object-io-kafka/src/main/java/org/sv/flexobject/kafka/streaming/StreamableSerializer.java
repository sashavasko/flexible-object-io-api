package org.sv.flexobject.kafka.streaming;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.SchemaException;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class StreamableSerializer implements Serializer<Streamable> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Streamable data) {
        return serialize(topic, null, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Streamable data) {
        if (data == null) {
            return null;
        }

        if (headers != null) {
            headers.add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));
            headers.add("__typeId__", data.getClass().getName().getBytes(StandardCharsets.UTF_8));
        }
        try {
            return data.toJsonBytes();
        } catch (Exception e) {
            throw new SchemaException("Failed to serialize instance of " + data.getClass().getName(), e);
        }
    }
}
