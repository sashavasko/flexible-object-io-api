package org.sv.flexobject.hadoop.utils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public interface IConfigured extends Configurable {
    // This is typically a major NO-NO, but is ok here, as configuration is the same within a process
    Configured confHolder = new Configured();

    default void reconfigure(){};

    static void setSharedConf(Configuration conf){
        confHolder.setConf(conf);
    }

    @Override
    default void setConf(Configuration conf){
        if (conf != null) {
            confHolder.setConf(conf);
            reconfigure();
        }
    }

    @Override
    default Configuration getConf(){
        return confHolder.getConf();
    }
}
