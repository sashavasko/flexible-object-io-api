package org.sv.flexobject.hadoop.streaming.parquet.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.InputFile;
import org.sv.flexobject.hadoop.streaming.parquet.AbstractParquetSource;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.read.json.JsonParquetReaderBuilder;

import java.io.IOException;

public class ParquetJsonSource extends AbstractParquetSource<ObjectNode> {

    public static Builder<ObjectNode, ParquetJsonSource> builder() {
        return new Builder<ObjectNode, ParquetJsonSource>(ParquetJsonSource.class) {

            @Override
            protected SchemedParquetReaderBuilder<ObjectNode, ?> makeBuilder(Configuration conf, Path filePath)  throws IOException {
                return new JsonParquetReaderBuilder(conf, filePath);
            }

            @Override
            protected SchemedParquetReaderBuilder<ObjectNode, ?> makeBuilder(InputFile file) {
                return new JsonParquetReaderBuilder(file);
            }
        };
    }
}
