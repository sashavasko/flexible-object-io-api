package org.sv.flexobject.hadoop.streaming.parquet.write.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.hadoop.streaming.parquet.write.ParquetWriteException;

public class JsonParquetException extends ParquetWriteException {
    JsonNode node;

    public JsonParquetException(JsonNode node, String fieldName) {
        super(fieldName);
        this.node = node;
    }

    public JsonParquetException(String message, JsonNode node, String fieldName) {
        super(message, fieldName);
        this.node = node;
    }

    public JsonParquetException(String message, Throwable cause, JsonNode node, String fieldName) {
        super(message, cause, fieldName);
        this.node = node;
    }

    @Override
    public String formatOptionalData() {
        return "node=" + node;
    }
}
