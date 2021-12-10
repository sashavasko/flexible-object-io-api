package org.sv.flexobject.hadoop.streaming.parquet.streamable;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.OutputFile;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.AbstractParquetSink;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedParquetWriterBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.write.streamable.ParquetWriterBuilder;

public class ParquetSink extends AbstractParquetSink<Streamable> {

    public static Builder<Streamable, ParquetSink> builder() {
        return new Builder<Streamable, ParquetSink>(ParquetSink.class) {

            @Override
            protected SchemedParquetWriterBuilder<Streamable, ?> makeBuilder(Path filePath) {
                return new ParquetWriterBuilder(filePath);
            }

            @Override
            protected SchemedParquetWriterBuilder<Streamable, ?> makeBuilder(OutputFile file) {
                return new ParquetWriterBuilder(file);
            }
        };
    }
}
