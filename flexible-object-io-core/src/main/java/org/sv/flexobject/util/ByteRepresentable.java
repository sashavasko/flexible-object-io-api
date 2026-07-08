package org.sv.flexobject.util;

import java.io.IOException;

public interface ByteRepresentable {

    void fromBytes(byte[] bytes, int offset, int length);
    byte[] toBytes() throws IOException;
}
