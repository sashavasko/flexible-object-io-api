package org.sv.flexobject.hadoop.mapreduce.input;

public interface InputConfOwner {

    void setInputConf(InputConf conf);

    InputConf getInputConf();

}
