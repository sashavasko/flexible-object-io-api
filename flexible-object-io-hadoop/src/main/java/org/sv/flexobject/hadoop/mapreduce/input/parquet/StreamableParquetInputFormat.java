package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReadSupport;

public class StreamableParquetInputFormat extends FilteredParquetInputFormat<Streamable> {
    public StreamableParquetInputFormat() {
        super(ParquetReadSupport.class);
    }
}
