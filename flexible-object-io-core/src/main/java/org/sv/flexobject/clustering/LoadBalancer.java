package org.sv.flexobject.clustering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.properties.Configurable;
import org.sv.flexobject.properties.PropertiesWrapper;

import java.util.Map;
import java.util.function.Function;

public class LoadBalancer implements Configurable {

    Logger logger = LogManager.getLogger(LoadBalancer.class);

    public static class Configuration extends PropertiesWrapper<Configuration> {
        public static long DEFAULT_BAD_RESPONSE_DELAY = 1000;
        public static long DEFAULT_NO_AVAILABLE_MEMBER_DELAY = 5000;

        long badResponseDelay = DEFAULT_BAD_RESPONSE_DELAY;
        long noAvailableMemberDelay = DEFAULT_NO_AVAILABLE_MEMBER_DELAY;
        Class<? extends LoadBalanceStrategy> strategyClass;
    }

    Cluster cluster;
    Configuration config = new Configuration();
    LoadBalanceStrategy strategy;

    long callCounter = 0;
    long masterCounter = 0;
    long slaveCounter = 0;
    long nullCounter = 0;

    Function<ClusterResponse, Boolean> abortOnResponse = null;
    Function<Long, Boolean> abortOnAttemptCount = null;
    Function<Long, Boolean> abortOnTimeout = null;

    public LoadBalanceStrategy getStrategy() {
        if (strategy == null && config.strategyClass != null) {
            try {
                strategy = config.strategyClass.newInstance();
            } catch (Exception e) {
                logger.error("Cannot instantiate Load Balancing Strategy", e);
            }
        }
        return strategy;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public void setStrategy(LoadBalanceStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public PropertiesWrapper getConfiguration() {
        return config;
    }

    @Override
    public void configure(Map props) throws Exception{
        config.from(props);

        if (strategy != null
                && config.strategyClass != null
                && !strategy.getClass().equals(config.strategyClass)) {
            strategy = null;
        }

        if (getStrategy() != null)
            strategy.configure(props);
    }

    public ClusterResponse handleRequest(ClusteredRequest request) {

        if (cluster == null){
            throw new RuntimeException("Cluster is not set");
        }else if (getStrategy() == null){
            throw new RuntimeException("Load Balancing Strategy is not set");
        }

        ClusterResponse response = null;
        callCounter++;
        long attempCount = 0;
        long startTimeNanos = System.nanoTime();
        do {
            attempCount++;
            try {
                ClusterMember source = getStrategy().selectSource(cluster);
                if (source != null) {
                    if (cluster.isMaster(source)) {
                        masterCounter++;
                        response = source.handleRequest(request);
                        logger.debug("MASTER:"
                                + masterCounter
                                + ":of:" + callCounter
                                + ":queryTime:" + source.getLastQueryTimeMicros());
                        strategy.adjustMasterProbability(cluster, source);
                    }else {
                        slaveCounter++;
                        response = source.handleRequest(request);
                        logger.debug("SLAVE" + source.getConnectionName() + ":"
                                + slaveCounter
                                + ":off:" + callCounter
                                + ":queryTime:" + source.getLastQueryTimeMicros());
                        strategy.adjustSlaveProbability(cluster, source);
                    }

                    if (response != null && !response.isError())
                        return response;

                    Thread.sleep(config.badResponseDelay);
                }else {
                    nullCounter++;
                    logger.debug("NULL:"
                            + nullCounter
                            + ":off:" + callCounter);
                    Thread.sleep(config.noAvailableMemberDelay);
                }
            } catch (InterruptedException e) {
                break;
            }

            if ((abortOnResponse != null && abortOnResponse.apply(response))
                    || (abortOnAttemptCount != null && abortOnAttemptCount.apply(attempCount))
                    || (abortOnTimeout != null && abortOnTimeout.apply(System.nanoTime() - startTimeNanos))) {
                break;
            }

        }while(true);

        return response;
    }

    public void setAbortOnResponse(Function<ClusterResponse, Boolean> abortOnResponse) {
        this.abortOnResponse = abortOnResponse;
    }

    public void setAbortOnAttemptCount(Function<Long, Boolean> abortOnAttemptCount) {
        this.abortOnAttemptCount = abortOnAttemptCount;
    }

    public void setAbortOnTimeout(Function<Long, Boolean> abortOnTimeout) {
        this.abortOnTimeout = abortOnTimeout;
    }

}
