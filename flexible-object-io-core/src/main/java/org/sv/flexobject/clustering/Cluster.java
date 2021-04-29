package org.sv.flexobject.clustering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.properties.Configurable;
import org.sv.flexobject.properties.NamespacePropertiesWrapper;
import org.sv.flexobject.properties.PropertiesWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Cluster implements AutoCloseable, Configurable {
    static Logger logger = LogManager.getLogger(Cluster.class);

    public static class Configuration extends NamespacePropertiesWrapper<Configuration> {

        public static long DEFAULT_OFFLINE_RETRY_MILLIS = 60000 * 5;

        public long offlineRetryMillis;

        public Configuration(String namespace) {
            super(namespace);
        }

        @Override
        public Configuration setDefaults() {
            offlineRetryMillis = DEFAULT_OFFLINE_RETRY_MILLIS;
            return this;
        }
    }

    ClusterMember master = null;
    List<ClusterMember> slaves = new ArrayList<>();
    List<ClusterMember> onlineSlaves = new ArrayList<>();
    Configuration config;

    public Cluster(String namespace) {
        this.config = new Configuration(namespace);
    }

    public void addMember(ClusterMember member, boolean isMaster){
        if (member != null) {
            if (isMaster)
                master = member;
            else
                slaves.add(member);
        }
    }

    @Override
    public void close() throws Exception {
        if (master != null) {
            logger.info("Shutting down master ...");
            master.close();
        }
        logger.info("Shutting down slaves ...");
        for (ClusterMember slave : slaves)
            slave.close();
        logger.info("Shutdown complete");
    }


    public boolean isMaster(ClusterMember source) {
        return master != null && source == master;
    }

    public ClusterMember getMaster() {
        return master;
    }

    boolean isMasterOnline(){
        return master != null
                && !master.isOffline(config.offlineRetryMillis);
    }

    public List<ClusterMember> getSlaves() {
        return slaves;
    }

    public Iterable<ClusterMember> onlineSlaves() {
        onlineSlaves.clear();
        for (int i = 0 ; i < slaves.size() ; ++i){
            ClusterMember slave = slaves.get(i);
            if (!slave.isOffline(config.offlineRetryMillis))
                onlineSlaves.add(slave);
        }
        return onlineSlaves;
    }

    public int countOnlineSlaves() {
        int count = 0;
        for (ClusterMember slave : slaves) {
            if (!slave.isOffline(config.offlineRetryMillis))
                count++;
        }
        return count;
    }

    @Override
    public void configure(Map props) throws Exception {
        config.from(props);
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "master=" + master +
                ", slaves=" + slaves +
                ", config=" + config +
                '}';
    }
}
