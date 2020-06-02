package org.sv.flexobject.stream.sources;

import org.sv.flexobject.stream.Source;

public class SingleValueSource<T> implements Source<T> {
    T value = null;
    boolean EOF = false;

    public SingleValueSource() {
        EOF = true;
    }

    public SingleValueSource(T value) {
        this.value = value;
        EOF = value == null;
    }

    public void setValue(T value) {
        this.value = value;
        EOF = false;
    }

    @Override
    public T get() throws Exception {
        EOF = true;
        return value;
    }

    @Override
    public boolean isEOF() {
        return EOF;
    }
}
