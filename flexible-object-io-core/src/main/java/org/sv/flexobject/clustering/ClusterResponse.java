package org.sv.flexobject.clustering;

public interface ClusterResponse {

    boolean isError();

    Exception getException();

    class SuccessOrFailure implements ClusterResponse{

        Exception exception;

        public SuccessOrFailure() {
            this.exception = null;
        }

        public SuccessOrFailure(Exception exception) {
            this.exception = exception;
        }

        @Override
        public boolean isError() {
            return exception != null;
        }

        @Override
        public Exception getException() {
            return exception;
        }
    }

    static SuccessOrFailure success(){
        return new SuccessOrFailure();
    }

    static SuccessOrFailure failure(Exception e){
        return new SuccessOrFailure(e);
    }

    static SuccessOrFailure failure(String error){
        return new SuccessOrFailure(new RuntimeException(error));
    }
}
