package org.sv.flexobject.io;

import org.sv.flexobject.Savable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class Consumer {

    private IOException exception = null;

    public void setException(Throwable e, Object datum) {
        this.exception = e instanceof IOException ? (IOException) e : IOException.saveIOError(e, datum);
    }

    public IOException getException() {
        return exception;
    }

    public abstract boolean consume(Savable datum);
    public abstract long getRecordsConsumed();

    public void cleanup() throws Exception {
        if (this instanceof AutoCloseable) {
            try {
                ((AutoCloseable) this).close();
            } catch (Exception e) {
                if (getException() == null)
                    setException(e, null);
            }
        }
        if (getException() != null)
            throw getException();
    }

    public long consumeAll(Iterable producer) throws Exception {
        for (Object datum : producer){
            consume((Savable)datum);
        }
        return getRecordsConsumed();
    }

    public List<Exception> getAllErrors(){
        if (getException() == null)
            return Collections.emptyList();
        return Arrays.asList(getException());
    }

}
