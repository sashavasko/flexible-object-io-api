package org.sv.flexobject.stream.sinks;

import org.sv.flexobject.util.ByteRepresentable;

import java.io.IOException;
import java.io.OutputStream;

public class ByteStreamSink<T extends ByteRepresentable> extends OutputStreamSink<T> {
    public ByteStreamSink(OutputStream os) {
        super(os);
    }

    @Override
    protected byte[] convert(T value) {
        try {
            return value.toBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
