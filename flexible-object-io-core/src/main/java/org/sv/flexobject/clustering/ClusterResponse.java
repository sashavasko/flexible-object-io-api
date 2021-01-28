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

    static ClusterResponse success(){
        return new SuccessOrFailure();
    }

    static ClusterResponse failure(Exception e){
        return new SuccessOrFailure(e);
    }

    static ClusterResponse failure(String error){
        return new SuccessOrFailure(new RuntimeException(error));
    }
}
