package org.sv.flexobject.io;

import org.sv.flexobject.Loadable;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class Producer<T extends Loadable> implements Iterable<T> {

    private IOException exception = null;

    abstract public T produce();

    public void setException(Throwable e) {
        this.exception = e instanceof IOException ? (IOException) e : IOException.loadIOError(e);
    }

    public IOException getException() {
        return exception;
    }

    public Stream stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public void ack(){
    }

    public void setEOF() {
    }

    public static class Error extends Producer {

        public Error(Throwable e) {
            setException(e);
        }

        @Override
        public Iterator iterator() {
            return new Iterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Object next() {
                    return null;
                }
            };
        }

        @Override
        public Loadable produce() {
            return null;
        }
    }

    public void cleanup() {
        if (this instanceof AutoCloseable) {
            try {
                ((AutoCloseable) this).close();
            } catch (Exception e) {
                if (getException() == null)
                    setException(e);
            }
        }
    }
}
