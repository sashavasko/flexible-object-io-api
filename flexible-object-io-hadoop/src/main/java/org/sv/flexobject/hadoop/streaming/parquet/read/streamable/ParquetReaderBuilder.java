package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.read.input.ByteArrayInputFile;

import java.io.IOException;

public class ParquetReaderBuilder extends SchemedParquetReaderBuilder<Streamable, ParquetReaderBuilder> {

    public static SchemedParquetReaderBuilder forPath(Configuration conf, Path file) throws IOException {
        return new ParquetReaderBuilder (conf, file);
    }

    public static SchemedParquetReaderBuilder forInput(InputFile file) {
        return new ParquetReaderBuilder(file);
    }

    public ParquetReaderBuilder(InputFile file) {
        super(file);
    }

    public ParquetReaderBuilder(byte[] bytes) {
        super(new ByteArrayInputFile(bytes));
    }

    public ParquetReaderBuilder(Configuration conf, Path path) throws IOException {
        super(HadoopInputFile.fromPath(path, conf));
        withConf(conf);
    }

    @Override
    protected ParquetReaderBuilder self() {
        return this;
    }

    @Override
    protected ReadSupport<Streamable> getReadSupport() {
        return new ParquetReadSupport(getSchema());
    }
}
