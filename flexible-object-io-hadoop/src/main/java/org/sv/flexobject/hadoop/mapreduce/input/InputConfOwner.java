package org.sv.flexobject.hadoop.mapreduce.input;

public interface InputConfOwner {

    void setInputConf(InputConf conf);

    <T extends InputConf> T getInputConf();

}
