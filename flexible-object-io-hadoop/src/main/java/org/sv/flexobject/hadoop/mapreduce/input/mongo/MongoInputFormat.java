package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import org.sv.flexobject.hadoop.mapreduce.input.ConfiguredInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.util.InstanceFactory;

public class MongoInputFormat<K,V> extends ConfiguredInputFormat<K,V> {
    @Override
    protected InputConf<InputConf> makeInputConf() {
        return InstanceFactory.get(MongoInputConf.class);
    }
}
