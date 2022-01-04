package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SimpleMRDriver extends MapReduceDriver<SimpleMRDriver>{

    public SimpleMRDriver() {
        setDriver(this);
    }

    @Override
    public void map(Object keyIn, Object valueIn, Mapper.Context context) throws IOException, InterruptedException {
    }
}
