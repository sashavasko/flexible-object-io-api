package org.sv.flexobject.hadoop.streaming.parquet.streamable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.InputFile;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.AbstractParquetSource;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReaderBuilder;

import java.io.IOException;

public class ParquetSource extends AbstractParquetSource<StreamableWithSchema> {

    public static Builder<StreamableWithSchema, ParquetSource> builder() {
        return new Builder<StreamableWithSchema, ParquetSource>(ParquetSource.class) {

            @Override
            protected SchemedParquetReaderBuilder<StreamableWithSchema, ?> makeBuilder(Configuration conf, Path filePath)  throws IOException {
                return new ParquetReaderBuilder(conf, filePath);
            }

            @Override
            protected SchemedParquetReaderBuilder<StreamableWithSchema, ?> makeBuilder(InputFile file) {
                return new ParquetReaderBuilder(file);
            }
        };
    }
}
