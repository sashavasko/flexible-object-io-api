package org.sv.flexobject.stream.sinks;


import org.sv.flexobject.stream.Sink;

import java.io.OutputStream;

public abstract class OutputStreamSink<T> implements Sink<T>, AutoCloseable {
    OutputStream os;
    boolean hasOutput = false;

    public OutputStreamSink(OutputStream os) {
        this.os = os;
    }

    public OutputStream outputStream() {
        return os;
    }

    abstract protected byte[] convert(T value);

    @Override
    public boolean put(T value) throws Exception {
        outputStream().write(convert(value));
        outputStream().write(System.lineSeparator().getBytes());
        hasOutput = true;
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
