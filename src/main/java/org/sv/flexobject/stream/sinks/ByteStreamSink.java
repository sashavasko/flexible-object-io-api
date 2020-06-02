package org.sv.flexobject.stream.sinks;

import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.util.ByteRepresentable;

import java.io.OutputStream;

public class ByteStreamSink<T extends ByteRepresentable> implements Sink<T>, AutoCloseable {
    OutputStream os;
    boolean hasOutput = false;

    public ByteStreamSink(OutputStream os) {
        this.os = os;
    }

    @Override
    public boolean put(T value) throws Exception {
        hasOutput = true;
        os.write(value.toBytes());
        os.write(System.lineSeparator().getBytes());
        return false;
    }

    @Override
    public void setEOF() {
        try {
            close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close OutputStream.", e);
        }
    }

    @Override
    public boolean hasOutput() {
        return hasOutput;
    }

    @Override
    public void close() throws Exception {
        os.close();
    }
}
