package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import org.junit.Test;
import org.sv.flexobject.hadoop.streaming.parquet.read.json.JsonReadSupport;

import static org.junit.Assert.assertEquals;

public class JsonParquetInputFormatTest {

    @Test
    public void testConstructor() {
        JsonParquetInputFormat format = new JsonParquetInputFormat();
        assertEquals(JsonReadSupport.class, format.getReadSupportClass());
    }
}