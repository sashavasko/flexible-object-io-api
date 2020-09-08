package org.sv.flexobject.hadoop.streaming.parquet.read.json;

import org.sv.flexobject.hadoop.streaming.parquet.read.input.ByteArrayInputFile;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import java.io.IOException;

public class JsonParquetReaderBuilder extends ParquetReader.Builder<JsonNode> {
    protected JsonParquetReaderBuilder(InputFile file) {
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
    protected ReadSupport<JsonNode> getReadSupport() {
        return new JsonReadSupport();
    }
}
