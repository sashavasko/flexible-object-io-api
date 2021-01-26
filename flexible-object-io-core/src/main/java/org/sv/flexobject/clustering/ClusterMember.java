package org.sv.flexobject.clustering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ClusterMember<Q extends ClusteredRequest, R extends ClusterResponse> implements AutoCloseable {

    Logger logger = LogManager.getLogger(ClusterMember.class);

    public enum State{
        active,
        failed,
        failedRepeated,
        temporaryOffline;

        long offlineTimeMillis;

        public State changeState(boolean isFailure){
            if (isFailure) {
                switch (this) {
                    case active:
                        return failed;
                    case failed:
                        return failedRepeated;
                    case failedRepeated:
                    case temporaryOffline:
                        offlineTimeMillis = System.currentTimeMillis();
                        return temporaryOffline;
                }
                return this;
            } else
                return active;
        }

        public long getOfflineTimeMillis() {
            return offlineTimeMillis;
        }
    }

    String connectionName;
    State state = State.active;
    int lastQueryTimeMicros = 0;
    int averageQueryTimeMicros = 0;

    long averageQueryTimeTotal = 0l;
    long averageQueryCount = 0;

    public ClusterMember() {
    }

    public ClusterMember(String connectionName) {
        setConnectionName(connectionName);
    }

    public abstract R getResponse(Q request);

    public R handleRequest(Q request) {
        R response;
        long startTime = System.nanoTime();
        response = getResponse(request);
        long endTime = System.nanoTime();

        state = state.changeState(response.isError());
        if (response.isError()){
            logError(response.getException());
        }

        if (state == State.active) {
            long queryTime = (endTime - startTime);
            lastQueryTimeMicros = (int) ( queryTime / 1000);
            if (averageQueryCount >= 3){
                averageQueryTimeMicros = (int) ((averageQueryTimeTotal / averageQueryCount) /1000);

                averageQueryTimeTotal = queryTime;
                averageQueryCount = 1;
            }else {
                averageQueryTimeTotal += queryTime;
                averageQueryCount++;
            }
        }

        return response;
    }

    synchronized private void logError(Exception exception) {
        logger.error("Error getting TCTs from " + connectionName + ":", exception);
        logger.info("State of " + connectionName + " changed to " + state);
    }

    public boolean isOffline(long retryTimeout){
        return state == State.temporaryOffline &&
                state.getOfflineTimeMillis() + retryTimeout > System.currentTimeMillis();
    }

    public boolean isFailed() {
        return state == State.failed || state == State.failedRepeated;
    }

    public int getLastQueryTimeMicros() {
        return lastQueryTimeMicros;
    }
    public int getAverageQueryTimeMicros() {
        return averageQueryTimeMicros;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getConnectionName() {
        return connectionName;
    }

    @Override
    public String toString() {
        return "ClusterMember{" +
                "connectionName='" + connectionName + '\'' +
                ", state=" + state +
                ", lastQueryTimeMicros=" + lastQueryTimeMicros +
                '}';
    }
}
