package org.sv.flexobject.hadoop.streaming.parquet.write.json;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonParquetException extends Exception {
    JsonNode node;
    String fieldName;

    public JsonParquetException(JsonNode node, String fieldName) {
        this.node = node;
        this.fieldName = fieldName;
    }

    public JsonParquetException(String message, JsonNode node, String fieldName) {
        super(message);
        this.node = node;
        this.fieldName = fieldName;
    }

    public JsonParquetException(String message, Throwable cause, JsonNode node, String fieldName) {
        super(message, cause);
        this.node = node;
        this.fieldName = fieldName;
    }

    @Override
    public String toString() {
        return "JsonParquetException: " +getLocalizedMessage() +
                " {node=" + node +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }
}
