package org.sv.flexobject.serde;

import org.sv.flexobject.Streamable;

import java.io.IOException;

public interface SerializationStrategy {

    byte[] serialize(Streamable datum) throws IOException;
    default void deserialize(Streamable datum, byte[] bytes) throws IOException{
        deserialize(datum, bytes, 0, bytes.length);
    }
    void deserialize(Streamable datum, byte[] bytes, int offset, int length) throws IOException;

    byte[] getContentType();
}
