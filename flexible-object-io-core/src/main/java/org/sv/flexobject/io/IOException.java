package org.sv.flexobject.io;

public class IOException extends Exception{

    public enum ERRORS {
        loadIOError,
        deleteIOError,
        saveIOError,
        processingError,
        unknownError
    }

    protected ERRORS error = ERRORS.unknownError;
    protected Object datum = null;
    protected String data = null;

    public IOException(Object datum, String message, ERRORS error) {
        super(message);
        this.datum = datum;
        this.error = error;
    }

    public IOException(Throwable cause, ERRORS error) {
        super(cause);
        this.error = error;
    }

    public IOException(Throwable cause, ERRORS error, Object datum) {
        this(cause, error);
        this.datum = datum;
    }

    public static IOException loadIOError(Throwable cause) {
        return new IOException(cause, ERRORS.loadIOError);
    }

    public static IOException saveIOError(Throwable cause, Object datum){
        return new IOException(cause, ERRORS.saveIOError, datum);
    }

    public static  IOException handleSaveException(Throwable e, Object datum) {
        if(e instanceof IOException)
            return (IOException)e;
        else
            return IOException.saveIOError(e, datum);
    }

    public static  IOException handleUnklnownException(Throwable e) {
        if(e instanceof IOException)
            return (IOException)e;
        else
            return new IOException (e, ERRORS.unknownError);
    }

    public ERRORS getError() {
        return error;
    }

    public String getData() {
        return data;
    }

    public Object getDatum() {
        return datum;
    }

    @Override
    public String toString() {
        return "IOException{" +
                "error=" + error +
                ", datum=" + datum +
                ", data=" + data +
                '}';
    }
}
