package org.sv.flexobject.stream.sinks;

import org.sv.flexobject.stream.Sink;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.Spliterator;
import java.util.function.Consumer;

public class QueueSink<T> implements Sink<T>, Iterable<T> {

    Queue<T> values;

    public QueueSink(Queue<T> values) {
        this.values = values;
    }

    public QueueSink() {
        values = new ArrayDeque<>();
    }

    @Override
    public boolean put(T value) throws Exception {
        values.add(value);
        return false;
    }

    @Override
    public boolean hasOutput() {
        return !values.isEmpty();
    }

    public Queue<T> getValues() {
        return values;
    }

    @Override
    public Iterator<T> iterator() {
        return values.iterator();
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        values.forEach(action);
    }

    @Override
    public Spliterator<T> spliterator() {
        return values.spliterator();
    }

    public void reset(){
        values.clear();
    }
}
