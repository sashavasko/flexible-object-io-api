package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.parquet.io.api.Binary;
import org.junit.Test;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.testdata.RawBinary;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class PrimitiveFieldConverterTest {

    @Test
    public void readBinaryField() {
        RawBinary testData = new RawBinary();
        PrimitiveFieldConverter converter = new PrimitiveFieldConverter(ParquetSchema.binaryField("binaryField"));
        converter.setCurrent(testData);

        converter.addBinary(Binary.fromConstantByteArray("foobar".getBytes(StandardCharsets.UTF_8)));

        assertEquals("foobar", new String(testData.binaryField));
    }

}