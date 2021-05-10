package org.sv.flexobject.hadoop.streaming.parquet.streamable;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.AbstractParquetSink;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSinkConf;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedParquetWriterBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.write.streamable.ParquetWriterBuilder;

public class ParquetSink extends AbstractParquetSink<StreamableWithSchema> {

    protected ParquetSink(ParquetSinkConf conf, ParquetWriter<StreamableWithSchema> writer) {
        super(conf, writer);
    }

    public static Builder<StreamableWithSchema, ParquetSink> builder() {
        return new Builder<StreamableWithSchema, ParquetSink>() {

            @Override
            protected SchemedParquetWriterBuilder<StreamableWithSchema, ?> makeBuilder(Path filePath) {
                return new ParquetWriterBuilder(filePath);
            }

            @Override
            protected SchemedParquetWriterBuilder<StreamableWithSchema, ?> makeBuilder(OutputFile file) {
                return new ParquetWriterBuilder(file);
            }

            @Override
            protected ParquetSink makeInstance(ParquetSinkConf conf, ParquetWriter<StreamableWithSchema> writer) {
                return new ParquetSink(conf, writer);
            }
        };
    }
}
