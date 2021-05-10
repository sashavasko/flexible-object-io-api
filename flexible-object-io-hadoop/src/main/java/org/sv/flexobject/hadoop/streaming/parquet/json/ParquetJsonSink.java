package org.sv.flexobject.hadoop.streaming.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.AbstractParquetSink;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSinkConf;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedParquetWriterBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.write.json.JsonParquetWriterBuilder;
import org.sv.flexobject.stream.Sink;

import java.io.IOException;

public class ParquetJsonSink extends AbstractParquetSink<JsonNode> {

    public static Builder<JsonNode, ParquetJsonSink> builder(){
        return new Builder<JsonNode, ParquetJsonSink>(ParquetJsonSink.class) {
            @Override
            protected SchemedParquetWriterBuilder<JsonNode, ?> makeBuilder(Path filePath) {
                return new JsonParquetWriterBuilder(filePath);
            }

            @Override
            protected SchemedParquetWriterBuilder<JsonNode, ?> makeBuilder(OutputFile file) {
                return new JsonParquetWriterBuilder(file);
            }
        };
    }
}
