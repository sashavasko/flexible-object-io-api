package org.sv.flexobject.stream.sources;

import org.sv.flexobject.stream.Source;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;

public class QueueSource<T> implements Source<T> {
    Queue<T> values;

    public QueueSource() {
    }

    public QueueSource(T ... values) {
        this.values = new ArrayDeque<>(Arrays.asList(values));
    }

    public QueueSource(Queue<T> values) {
        this.values = values;
    }

    @Override
    public T get() throws Exception {
        return values.remove();
    }

    @Override
    public boolean isEOF() {
        return values.peek() == null;
    }
}
