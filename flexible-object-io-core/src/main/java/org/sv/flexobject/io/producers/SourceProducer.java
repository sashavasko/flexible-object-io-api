package org.sv.flexobject.io.producers;


import org.sv.flexobject.Loadable;
import org.sv.flexobject.io.CloseableProducer;
import org.sv.flexobject.stream.Source;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SourceProducer<T extends Loadable> extends CloseableProducer<T> {
    Source<T> source;

    public SourceProducer(Source source) {
        this.source = source;
    }

    @Override
    public void unsafeClose() throws Exception {
        source.close();
    }

    public T produce(){
        T datum = null;

        try {
            datum = source.get();
            if (datum != null) {
                return datum;
            }
        } catch (Exception e) {
            setException(e);
        }
        return null;
    }

    @Override
    public void ack() {
        source.ack();
    }

    @Override
    public void setEOF() {
        source.setEOF();
    }

    public class SourceIterator implements Iterator<T>{

        T datum = null;

        @Override
        public boolean hasNext() {
            datum = produce();
            return datum != null;
        }

        @Override
        public T next() {
            if (datum == null)
                throw new NoSuchElementException();
            return datum;
        }
    }

    @Override
    public Iterator iterator() {
        return new SourceIterator();
    }

}
