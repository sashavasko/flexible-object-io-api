package org.sv.flexobject.kafka;

import org.sv.flexobject.Streamable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface KafkaStreamable {
    default byte[] getKafkaKey(){
        return null;
    }

    static byte[] toBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    default byte[] getKafkaValue(){
        return toBytes(toString());
    }
}
