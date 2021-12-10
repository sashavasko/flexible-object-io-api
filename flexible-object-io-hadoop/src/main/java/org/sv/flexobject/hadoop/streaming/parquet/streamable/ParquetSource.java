package org.sv.flexobject.hadoop.streaming.parquet.streamable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.InputFile;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.AbstractParquetSource;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReaderBuilder;

import java.io.IOException;

public class ParquetSource extends AbstractParquetSource<Streamable> {

    public static Builder<Streamable, ParquetSource> builder() {
        return new Builder<Streamable, ParquetSource>(ParquetSource.class) {

            @Override
            protected SchemedParquetReaderBuilder<Streamable, ?> makeBuilder(Configuration conf, Path filePath)  throws IOException {
                return new ParquetReaderBuilder(conf, filePath);
            }

            @Override
            protected SchemedParquetReaderBuilder<Streamable, ?> makeBuilder(InputFile file) {
                return new ParquetReaderBuilder(file);
            }
        };
    }
}
