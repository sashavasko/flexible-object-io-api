package org.sv.flexobject.io;

public abstract class CloseableProducer extends Producer implements  AutoCloseable {

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
