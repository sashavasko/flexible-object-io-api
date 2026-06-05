package org.sv.flexobject.kafka;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface KafkaStreamable {
    default byte[] getKafkaKey(){
        return null;
    }

    default byte[] getKafkaValue(){
        return toString().getBytes(StandardCharsets.UTF_8);
    }
}
