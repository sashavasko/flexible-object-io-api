package org.sv.flexobject.hadoop.streaming.parquet.read.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.read.input.ByteArrayInputFile;

import java.io.IOException;

public class JsonParquetReaderBuilder extends SchemedParquetReaderBuilder<ObjectNode, JsonParquetReaderBuilder> {
    public JsonParquetReaderBuilder(InputFile file) {
        super(file);
    }

    @Override
    protected JsonParquetReaderBuilder self() {
        return this;
    }

    public JsonParquetReaderBuilder(byte[] bytes) {
        super(new ByteArrayInputFile(bytes));
    }

    public JsonParquetReaderBuilder(Configuration conf, Path path) throws IOException {
        super(HadoopInputFile.fromPath(path, conf));
        withConf(conf);
    }

    @Override
    protected ReadSupport<ObjectNode> getReadSupport() {
        return new JsonReadSupport(getSchema());
    }
}
