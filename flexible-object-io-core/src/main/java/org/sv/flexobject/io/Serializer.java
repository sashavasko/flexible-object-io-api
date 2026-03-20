package org.sv.flexobject.io;


import org.sv.flexobject.Streamable;

public interface Serializer {
    byte[] ser(Streamable datum) throws java.io.IOException;
    void deser(Streamable datum, byte[] bytes) throws java.io.IOException;
}
