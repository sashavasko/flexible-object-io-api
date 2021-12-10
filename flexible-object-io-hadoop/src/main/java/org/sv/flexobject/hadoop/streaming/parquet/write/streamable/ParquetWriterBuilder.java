package org.sv.flexobject.hadoop.streaming.parquet.write.streamable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedParquetWriterBuilder;

public class ParquetWriterBuilder extends SchemedParquetWriterBuilder<Streamable, ParquetWriterBuilder> {

    public static SchemedParquetWriterBuilder forPath(Path file) {
        return new ParquetWriterBuilder (file);
    }

    public static SchemedParquetWriterBuilder forOutput(OutputFile file) {
        return new ParquetWriterBuilder (file);
    }

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
    protected WriteSupport<Streamable> getWriteSupport(Configuration configuration) {
        return new ParquetWriteSupport(getSchema());
    }
}
