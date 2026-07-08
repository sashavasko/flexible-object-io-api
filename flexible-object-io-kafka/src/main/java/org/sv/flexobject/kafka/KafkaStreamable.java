package org.sv.flexobject.kafka;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.serde.SerializationStrategy;
import org.sv.flexobject.util.ByteRepresentable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public interface KafkaStreamable extends Streamable{
    default byte[] getKafkaKey() throws IOException{
        return null;
    }

    default byte[] getKafkaValue(SerializationStrategy serde) throws IOException {
        return serde.serialize(this);
    }
}
