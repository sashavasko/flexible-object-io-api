package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.read.input.ByteArrayInputFile;
import org.sv.flexobject.hadoop.streaming.parquet.read.json.JsonReadSupport;

import java.io.IOException;

public class ParquetReaderBuilder extends SchemedParquetReaderBuilder<StreamableWithSchema, ParquetReaderBuilder> {
    public ParquetReaderBuilder(InputFile file) {
        super(file);
    }

    @Override
    protected ParquetReaderBuilder self() {
        return this;
    }

    public ParquetReaderBuilder(byte[] bytes) {
        super(new ByteArrayInputFile(bytes));
    }

    public ParquetReaderBuilder(Configuration conf, Path path) throws IOException {
        super(HadoopInputFile.fromPath(path, conf));
        withConf(conf);
    }

    @Override
    protected ReadSupport<StreamableWithSchema> getReadSupport() {
        return new ParquetReadSupport(getSchema());
    }
}
