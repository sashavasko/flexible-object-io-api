package org.sv.flexobject.hadoop.streaming.parquet.write.streamable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedParquetWriterBuilder;

public class ParquetWriterBuilder extends SchemedParquetWriterBuilder<StreamableWithSchema, ParquetWriterBuilder> {

    public ParquetWriterBuilder(Path path) {
        super(path);
    }

    public ParquetWriterBuilder(OutputFile file) {
        super(file);
    }

    @Override
    protected ParquetWriterBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<StreamableWithSchema> getWriteSupport(Configuration configuration) {
        return new ParquetWriteSupport(getSchema());
    }
}
