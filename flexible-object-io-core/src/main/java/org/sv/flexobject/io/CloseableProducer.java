package org.sv.flexobject.io;

import org.sv.flexobject.Loadable;

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
}
