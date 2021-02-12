package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import org.apache.parquet.hadoop.ParquetInputFormat;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReadSupport;

public class StreamableParquetInputFormat extends ParquetInputFormat<StreamableWithSchema> {

    public StreamableParquetInputFormat() {
        super(ParquetReadSupport.class);
    }
}
