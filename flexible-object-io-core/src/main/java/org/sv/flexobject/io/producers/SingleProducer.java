package org.sv.flexobject.io.producers;

import org.sv.flexobject.Loadable;
import org.sv.flexobject.io.Producer;

import java.util.Iterator;

public class SingleProducer<T extends Loadable> extends Producer<T> {

    T datum;

    public SingleProducer(T datum) {
        this.datum = datum;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            boolean done = false;

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public T next() {
                done = true;
                return datum;
            }
        };
    }

    @Override
    public T produce() {
        T toReturn = datum;
        datum = null;
        return toReturn;
    }
}
