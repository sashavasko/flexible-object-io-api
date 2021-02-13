package org.sv.flexobject.hadoop.streaming.parquet.read.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.read.input.ByteArrayInputFile;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public class JsonParquetReaderBuilder extends SchemedParquetReaderBuilder<ObjectNode, JsonParquetReaderBuilder> {

    public static SchemedParquetReaderBuilder forPath(Configuration conf, Path file) throws IOException {
        return new JsonParquetReaderBuilder(conf, file);
    }

    public static SchemedParquetReaderBuilder forInput(InputFile file) {
        return new JsonParquetReaderBuilder (file);
    }

    public JsonParquetReaderBuilder(InputFile file) {
        super(file);
    }

    public JsonParquetReaderBuilder(byte[] bytes) {
        super(new ByteArrayInputFile(bytes));
    }

    public JsonParquetReaderBuilder(Configuration conf, Path path) throws IOException {
        super(HadoopInputFile.fromPath(path, conf));
        withConf(conf);
    }

    @Override
    protected JsonParquetReaderBuilder self() {
        return this;
    }

    @Override
    protected ReadSupport<ObjectNode> getReadSupport() {
        return new JsonReadSupport(getSchema());
    }
}
