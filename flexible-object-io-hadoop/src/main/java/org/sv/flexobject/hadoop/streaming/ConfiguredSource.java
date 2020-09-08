package org.sv.flexobject.hadoop.streaming;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.stream.Source;

public abstract class ConfiguredSource<T> implements Source<T>, Configurable {
    Configuration configuration;

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}
