package org.sv.flexobject.dremio.api;

public class DremioApiException extends RuntimeException{

    int restApiReturnCode;

    public DremioApiException() {
    }

    public DremioApiException(String message) {
        super(message);
    }

    public DremioApiException(String message, int restApiReturnCode) {
        super(message);
        this.restApiReturnCode = restApiReturnCode;
    }

    public DremioApiException(String message, Throwable cause) {
        super(message, cause);
    }

    public DremioApiException(Throwable cause) {
        super(cause);
    }

    public DremioApiException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public int getRestApiReturnCode() {
        return restApiReturnCode;
    }

    public void setRestApiReturnCode(int restApiReturnCode) {
        this.restApiReturnCode = restApiReturnCode;
    }
}
