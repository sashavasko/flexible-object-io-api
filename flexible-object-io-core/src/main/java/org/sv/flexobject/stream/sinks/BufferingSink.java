package org.sv.flexobject.stream.sinks;

import com.carfax.dt.streaming.Streamable;
import com.carfax.dt.streaming.stream.Sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class BufferingSink<T> implements Sink<T> {

    List<T> buffer;

    public BufferingSink() {
        buffer = new ArrayList<>();
    }

    public BufferingSink(List<T> buffer) {
        this.buffer = buffer;
    }

    @Override
    public boolean put(T value) throws Exception {
        return buffer.add(value);
    }

    @Override
    public boolean hasOutput() {
        return !buffer.isEmpty();
    }

    public List<T> getBuffer() {
        return buffer;
    }
}
