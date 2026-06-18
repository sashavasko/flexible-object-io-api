package org.sv.flexobject.kafka;

import org.sv.flexobject.util.ByteRepresentable;

import java.nio.charset.StandardCharsets;

public interface KafkaStreamable {
    default byte[] getKafkaKey(){
        return null;
    }

    static byte[] toBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    default byte[] getKafkaValue(){
        if (this instanceof ByteRepresentable)
            return ((ByteRepresentable)this).toBytes();

        return toBytes(toString());
    }
}
