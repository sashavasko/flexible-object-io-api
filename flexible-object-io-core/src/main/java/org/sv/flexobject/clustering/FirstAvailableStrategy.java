package org.sv.flexobject.clustering;

import java.util.Map;

public class FirstAvailableStrategy implements LoadBalanceStrategy {
    @Override
    public ClusterMember selectSource(Cluster cluster) {
        if (cluster.isMasterOnline())
            return cluster.getMaster();
        for (ClusterMember slave : cluster.onlineSlaves())
            return slave;
        return null;
    }

    @Override
    public void adjustMasterProbability(Cluster cluster, ClusterMember member) {

    }

    @Override
    public void adjustSlaveProbability(Cluster cluster, ClusterMember source) {

    }

    @Override
    public void configure(Map props) throws Exception {

    }
}
