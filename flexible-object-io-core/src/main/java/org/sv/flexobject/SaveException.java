package org.sv.flexobject;

public class SaveException extends RuntimeException {

    public static class NoRowsAffectedException extends SaveException {
        public NoRowsAffectedException() {
            super("Failed to save record - 0 rows affected.");
        }
    }

    public SaveException() {
    }

    public SaveException(String message) {
        super(message);
    }

    public SaveException(String message, Throwable cause) {
        super(message, cause);
    }

    public SaveException(Throwable cause) {
        super(cause);
    }

    public SaveException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
