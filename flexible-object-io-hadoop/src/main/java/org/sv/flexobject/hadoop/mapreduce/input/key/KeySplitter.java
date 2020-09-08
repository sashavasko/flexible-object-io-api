package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.List;

public interface KeySplitter {
    List<InputSplit> split(Configuration conf);
}
