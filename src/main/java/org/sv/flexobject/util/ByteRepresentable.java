package org.sv.flexobject.util;

public interface ByteRepresentable {

    void fromBytes(byte[] bytes, int length);
    byte[] toBytes();
}
