package org.sv.flexobject.stream;


import org.sv.flexobject.Loadable;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SourceIterator<T extends Loadable> implements Iterator<T> {
    Source<T> source;
    T datum = null;

    public SourceIterator(Source<T> source) {
        this.source = source;
    }

    @Override
    public boolean hasNext() {
        try {
            datum = source.get();
        } catch (Exception e) {
            if (e instanceof RuntimeException)
                throw (RuntimeException)e;
            throw new RuntimeException(e);
        }
        return datum != null;
    }

    @Override
    public T next() {
        if (datum == null)
            throw new NoSuchElementException();
        return datum;
    }
}
