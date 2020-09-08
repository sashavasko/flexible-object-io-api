package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.mapreduce.util.MRDao;

public interface BatchInputDao extends MRDao {

    InAdapter startBatch(long startKey, long batchSize) throws Exception;
}
