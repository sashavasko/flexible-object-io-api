package org.sv.flexobject.clustering;

import org.sv.flexobject.properties.Configurable;

import java.util.Map;

public interface LoadBalanceStrategy extends Configurable {

    ClusterMember selectSource(Cluster cluster);

    void adjustMasterProbability(Cluster cluster, ClusterMember member);

    void adjustSlaveProbability(Cluster cluster, ClusterMember source);
}
