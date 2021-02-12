package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public interface IReducerHelper {

        String REDUCER_HELPER_CONFIG = "mapreduce.reduce.helper.class";

        default void setupReducer(Reducer.Context context) throws IOException, InterruptedException {};
        default void reduce(WritableComparable keyIn, Iterable<Writable> values, Reducer.Context context) throws IOException, InterruptedException {};
        default void cleanupReducer(Reducer.Context context) throws IOException, InterruptedException {};
}
