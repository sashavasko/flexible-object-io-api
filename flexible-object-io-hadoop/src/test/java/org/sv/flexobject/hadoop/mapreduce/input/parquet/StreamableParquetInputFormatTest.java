package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import org.junit.jupiter.api.Test;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReadSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamableParquetInputFormatTest {

    @Test
    public void readSupport() {
        StreamableParquetInputFormat format = new StreamableParquetInputFormat();
        assertEquals(ParquetReadSupport.class, format.getReadSupportClass());
    }
}