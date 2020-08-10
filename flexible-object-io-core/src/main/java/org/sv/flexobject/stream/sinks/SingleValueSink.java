package org.sv.flexobject.stream.sinks;

import org.sv.flexobject.stream.Sink;

public class SingleValueSink<T> implements Sink<T> {

    T value = null;

    @Override
    public boolean put(T value) throws Exception {
        this.value = value;
        return true;
    }

    @Override
    public boolean hasOutput() {
        return value != null;
    }

    public T get() {
        T valueToReturn = value;
        value = null;
        return valueToReturn;
    }

}