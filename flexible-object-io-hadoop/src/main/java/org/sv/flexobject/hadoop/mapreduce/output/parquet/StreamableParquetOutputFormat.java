package org.sv.flexobject.hadoop.mapreduce.output.parquet;

import org.apache.parquet.hadoop.api.WriteSupport;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.write.streamable.ParquetWriteSupport;

public class StreamableParquetOutputFormat extends ParquetNoEmptyOutputFormat<Streamable> {
    public <S extends WriteSupport<Streamable>> StreamableParquetOutputFormat(S writeSupport) {
        super(writeSupport);
    }

    public StreamableParquetOutputFormat() {
        super(new ParquetWriteSupport());
    }

    public StreamableParquetOutputFormat(Class<? extends Streamable> dataClass) {
        super(new ParquetWriteSupport(ParquetSchema.forClass(dataClass)));
    }
}
