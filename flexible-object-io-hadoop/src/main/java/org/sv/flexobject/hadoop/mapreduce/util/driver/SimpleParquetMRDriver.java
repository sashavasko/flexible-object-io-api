package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SimpleParquetMRDriver extends ParquetMapReduceDriver<SimpleParquetMRDriver>{
    public SimpleParquetMRDriver() {
        setDriver(this);
    }

    @Override
    public void map(Object keyIn, Object valueIn, Mapper.Context context) throws IOException, InterruptedException {

    }
}
