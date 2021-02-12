package org.sv.flexobject.hadoop.mapreduce.output.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.sv.flexobject.hadoop.streaming.parquet.write.json.JsonWriteSupport;

public class JsonParquetOutputFormat extends ParquetNoEmptyOutputFormat<JsonNode> {
    public <S extends WriteSupport<JsonNode>> JsonParquetOutputFormat(S writeSupport) {
        super(writeSupport);
    }

    public JsonParquetOutputFormat() {
        super(new JsonWriteSupport());
    }

}
