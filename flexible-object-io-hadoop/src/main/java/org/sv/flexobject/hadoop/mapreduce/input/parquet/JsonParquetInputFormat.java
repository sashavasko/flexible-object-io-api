package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.sv.flexobject.hadoop.streaming.parquet.read.json.JsonReadSupport;

public class JsonParquetInputFormat extends FilteredParquetInputFormat<ObjectNode> {
    public JsonParquetInputFormat() {
        super(JsonReadSupport.class);
    }
}
