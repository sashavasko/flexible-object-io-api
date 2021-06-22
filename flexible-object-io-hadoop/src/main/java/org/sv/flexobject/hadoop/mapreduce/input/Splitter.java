package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.util.List;

public interface Splitter {
    List<InputSplit> split(Configuration conf) throws IOException;
}
