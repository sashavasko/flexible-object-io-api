package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sv.flexobject.stream.Source;

import java.io.IOException;

public interface SourceBuilder {

    Source build(InputSplit split, TaskAttemptContext context) throws InstantiationException, IllegalAccessException, IllegalArgumentException, IOException;

}
