package org.sv.flexobject.hadoop.streaming.parquet.write.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedParquetWriterBuilder;

public class JsonParquetWriterBuilder extends SchemedParquetWriterBuilder<JsonNode, JsonParquetWriterBuilder> {

    public JsonParquetWriterBuilder(Path path) {
        super(path);
    }

    public JsonParquetWriterBuilder(OutputFile file) {
        super(file);
    }

    @Override
    protected JsonParquetWriterBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<JsonNode> getWriteSupport(Configuration configuration) {
        return new JsonWriteSupport(getSchema());
    }
}
