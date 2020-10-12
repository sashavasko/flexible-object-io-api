package org.sv.flexobject.hadoop.streaming.parquet.write.streamable;

import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.streaming.TestDataWithInferredSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.write.output.ByteArrayOutputFile;

import java.io.IOException;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class ParquetWriterBuilderTest {

    @Test
    public void writeObjectToByteArray() throws Exception {
        ByteArrayOutputFile byteArrayOutput = new ByteArrayOutputFile();
        ParquetWriter writer = new ParquetWriterBuilder(byteArrayOutput).withSchema(TestDataWithInferredSchema.class).build();
        TestDataWithInferredSchema testData = TestDataWithInferredSchema.random(true);

        System.out.println(testData.intMap);

        System.out.println(ParquetSchema.forClass(TestDataWithInferredSchema.class));
        writer.write(testData);


    }
}