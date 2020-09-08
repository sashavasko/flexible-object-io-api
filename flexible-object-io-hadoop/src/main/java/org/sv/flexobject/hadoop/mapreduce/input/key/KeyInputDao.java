package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.io.Writable;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.mapreduce.util.MRDao;

public interface KeyInputDao<T extends Writable>  extends MRDao {

    InAdapter start(T key) throws Exception;
}
