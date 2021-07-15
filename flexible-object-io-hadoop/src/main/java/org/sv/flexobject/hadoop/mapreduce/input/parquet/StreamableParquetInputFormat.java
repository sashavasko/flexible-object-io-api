package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReadSupport;

import java.io.IOException;

public class StreamableParquetInputFormat extends FilteredParquetInputFormat<StreamableWithSchema> {
    public StreamableParquetInputFormat() {
        super(ParquetReadSupport.class);
    }
}
