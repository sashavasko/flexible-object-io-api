package org.sv.flexobject.clustering;

import java.util.Map;

public interface LoadBalanceStrategy {

    ClusterMember selectSource(Cluster cluster);

    void adjustMasterProbability(Cluster cluster, ClusterMember member);

    void adjustSlaveProbability(Cluster cluster, ClusterMember source);

    void configure(Map props) throws Exception;
}
