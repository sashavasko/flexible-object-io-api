package org.sv.flexobject.io;

import org.sv.flexobject.Loadable;

import java.util.Iterator;

public abstract class CloseableProducer<T extends Loadable> extends Producer<T> implements  AutoCloseable {

    public abstract void unsafeClose() throws Exception;

    @Override
    public void close() {
        try {
            unsafeClose();
        }catch (Exception e){
            if (getException() == null)
                setException(e);
        }
    }
    public static class Error extends CloseableProducer {

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

        @Override
        public void unsafeClose() throws Exception {
        }
    }
}
