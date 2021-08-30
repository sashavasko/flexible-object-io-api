package org.sv.flexobject.clustering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.properties.NamespacePropertiesWrapper;
import org.sv.flexobject.properties.PropertiesWrapper;
import org.sv.flexobject.util.InstanceFactory;
import org.sv.flexobject.util.SeededRandom;

import java.util.Map;
import java.util.Random;

public class ProbabilityBasedThrottlingStrategy implements LoadBalanceStrategy {

    static Logger logger = LogManager.getLogger(ProbabilityBasedThrottlingStrategy.class);

    public static class Configuration extends NamespacePropertiesWrapper<Configuration> {
        public static final Long DEFAULT_MASTER_UNDERLOADED_RESPONSE_MICROS = 6l*1000;
        public static final Long DEFAULT_MASTER_CONCERNING_RESPONSE_MICROS = 13l*1000;
        public static final Long DEFAULT_MASTER_OVERLOADED_RESPONSE_MICROS = 30l*1000;
        public static final Double DEFAULT_MASTER_CONCERNED_COEFFICIENT = 0.8;
        public static final Double DEFAULT_MASTER_OVERLOADED_COEFFICIENT = 0.6;
        public static final Double DEFAULT_MASTER_PROBABILITY = 0.95;
        public static final Double DEFAULT_MIN_MASTER_PROBABILITY = 0.3;
        public static final Double DEFAULT_MASTER_RECOVERY_COEFFICIENT = 1.1;
        public static final Double DEFAULT_DAMPENING_FACTOR = 0.8;
        public static final Long DEFAULT_FIRST_AVAILABLE_SLAVE_THRESHOLD_MICROS = 60*1000l;
        public static final Double DEFAULT_USE_RUNNING_AVERAGE_THRESHOLD = 0.7;

        protected long masterUnderloadedResponseMicros;
        protected long masterConcerningResponseMicros;
        protected long masterOverloadedResponseMicros;

        protected double masterConcernedCoefficient;
        protected double masterOverloadedCoefficient;
        protected double masterRecoveryCoefficient;
        protected double maxMasterProbability;
        protected double minMasterProbability;
        protected double dampeningFactor;
        protected long firstAvailableSlaveThresholdMicros;
        protected double useRunningAverageThreshold;

        public Configuration() {
            this(Namespace.getDefaultNamespace());
        }
        public Configuration(Namespace parent) {
            super(parent, "strategy");
        }

        @Override
        public Configuration setDefaults() {
            masterUnderloadedResponseMicros = DEFAULT_MASTER_UNDERLOADED_RESPONSE_MICROS;
            masterConcerningResponseMicros = DEFAULT_MASTER_CONCERNING_RESPONSE_MICROS;
            masterOverloadedResponseMicros = DEFAULT_MASTER_OVERLOADED_RESPONSE_MICROS;

            masterConcernedCoefficient = DEFAULT_MASTER_CONCERNED_COEFFICIENT;
            masterOverloadedCoefficient = DEFAULT_MASTER_OVERLOADED_COEFFICIENT;
            masterRecoveryCoefficient = DEFAULT_MASTER_RECOVERY_COEFFICIENT;
            maxMasterProbability = DEFAULT_MASTER_PROBABILITY;
            minMasterProbability = DEFAULT_MIN_MASTER_PROBABILITY;
            dampeningFactor = DEFAULT_DAMPENING_FACTOR;
            firstAvailableSlaveThresholdMicros = DEFAULT_FIRST_AVAILABLE_SLAVE_THRESHOLD_MICROS;
            useRunningAverageThreshold = DEFAULT_USE_RUNNING_AVERAGE_THRESHOLD;
            return this;
        }
    }


    Random random;
    Configuration config;
    double currentMasterProbability;

    protected ProbabilityBasedThrottlingStrategy() {
    }

    public static class Builder{
        Cluster cluster;
        Random random = new Random(System.nanoTime());
        Map properties;

        public Builder forCluster(Cluster cluster){
            this.cluster = cluster;
            return this;
        }

        public Builder withRandomGenerator(Random randomNumberGenerator){
            this.random = randomNumberGenerator;
            return this;
        }

        public Builder withProperties(Map props){
            this.properties = props;
            return this;
        }

        public LoadBalanceStrategy build(){
            ProbabilityBasedThrottlingStrategy strategy = new ProbabilityBasedThrottlingStrategy();
            strategy.config = new Configuration(cluster.getConfiguration().getNamespace());
            strategy.random = random;

            if (properties != null) {
                try {
                    strategy.configure(properties);
                } catch (Exception e) {
                    logger.error("Failed to configure from supplied properties", e);
                }
            }

            strategy.currentMasterProbability = strategy.config.maxMasterProbability;
            return strategy;
        }

    }

    public static Builder builder(){
        return new Builder();
    }

    @Override
    public ClusterMember selectSource(Cluster cluster) {
        int onlineSlavesCount = cluster.countOnlineSlaves();

        if (onlineSlavesCount > 0) {
            return isItMasterTurn(cluster) ? cluster.getMaster() : selectOnlineSlaveRandomly(cluster, 1.0/onlineSlavesCount);
        }
        logger.trace("No slaves available. Master is " + (cluster.isMasterOnline() ? "online" : "offline"));
        return cluster.isMasterOnline() ? cluster.getMaster() : null;
    }

    synchronized private ClusterMember selectOnlineSlaveRandomly(Cluster cluster, double slaveProbabilityIncrement) {
        double slaveProbability = slaveProbabilityIncrement;

        ClusterMember lastOnlineSlave = null;
        for (ClusterMember slave : cluster.onlineSlaves()) {
            int queryTime = (1 - currentMasterProbability) > config.useRunningAverageThreshold
                    ? slave.getAverageQueryTimeMicros()
                    : slave.getLastQueryTimeMicros();

            if (config.firstAvailableSlaveThresholdMicros > queryTime
                    || random.nextDouble() < slaveProbability){
                logger.trace("Selected Slave : " + slave);
                return slave;
            }

            lastOnlineSlave = slave;
            slaveProbability += slaveProbabilityIncrement;
        }

        logger.trace("Selected Slave : " + lastOnlineSlave);
        return lastOnlineSlave;
    }

    private double adjustMasterProbabilityBasedOnLastQueryTime(ClusterMember master) {
        double masterProbability = currentMasterProbability;

        if (master != null && random.nextDouble() < config.dampeningFactor) {

            int queryTime = masterProbability > config.useRunningAverageThreshold
                    ? master.getAverageQueryTimeMicros()
                    : master.getLastQueryTimeMicros();

            if (queryTime > config.masterOverloadedResponseMicros)
                masterProbability = masterProbability * config.masterOverloadedCoefficient;
            else if (queryTime > config.masterConcerningResponseMicros)
                masterProbability = masterProbability * config.masterConcernedCoefficient;
            else if (queryTime > config.masterUnderloadedResponseMicros){
                masterProbability = masterProbability * config.masterRecoveryCoefficient;
            }

            if (masterProbability > config.maxMasterProbability)
                return config.maxMasterProbability;
            else if (masterProbability < config.minMasterProbability)
                return config.minMasterProbability;
        }
        return masterProbability;
    }



    @Override
    public void adjustMasterProbability(Cluster cluster, ClusterMember member) {
        double newMasterProbability = adjustMasterProbabilityBasedOnLastQueryTime(member);
        if ((int)(newMasterProbability*10) != (int)(currentMasterProbability *10)) {
            logger.info("Adjusted master probability to :" + newMasterProbability);
        }
        currentMasterProbability = newMasterProbability;

    }

    @Override
    public void adjustSlaveProbability(Cluster cluster, ClusterMember source) {

    }

    @Override
    public void configure(Map props) throws Exception {
        config.from(props);
    }

    @Override
    public NamespacePropertiesWrapper getConfiguration() {
        if (config == null)
            config = new Configuration();
        return config;
    }

    protected boolean isItMasterTurn(Cluster cluster){
        return cluster.isMasterOnline()
                && random.nextDouble() < currentMasterProbability;
    }
}
