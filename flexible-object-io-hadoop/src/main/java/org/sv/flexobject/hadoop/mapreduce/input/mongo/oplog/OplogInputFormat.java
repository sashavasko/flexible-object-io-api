package org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog;


import org.sv.flexobject.hadoop.mapreduce.input.ConfiguredInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.util.InstanceFactory;

public class OplogInputFormat<K,V> extends ConfiguredInputFormat<K,V> {
    @Override
    protected InputConf<InputConf> makeInputConf() {
        return InstanceFactory.get(OplogInputConf.class);
    }
}
