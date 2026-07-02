package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import org.junit.jupiter.api.Test;
import org.sv.flexobject.hadoop.streaming.parquet.read.json.JsonReadSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonParquetInputFormatTest {

    @Test
    public void testConstructor() {
        JsonParquetInputFormat format = new JsonParquetInputFormat();
        assertEquals(JsonReadSupport.class, format.getReadSupportClass());
    }
}