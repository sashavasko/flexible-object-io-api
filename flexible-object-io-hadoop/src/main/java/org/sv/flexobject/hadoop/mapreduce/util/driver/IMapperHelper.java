package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public interface IMapperHelper {

    String MAPPER_HELPER_CONFIG = "mapreduce.map.helper.class";

    default void setupMapper(Mapper.Context context)  throws IOException, InterruptedException  {};
    void map(Object keyIn, Object valueIn, Mapper.Context context) throws IOException, InterruptedException;
    default void cleanupMapper(Mapper.Context context) throws IOException, InterruptedException {};
}
