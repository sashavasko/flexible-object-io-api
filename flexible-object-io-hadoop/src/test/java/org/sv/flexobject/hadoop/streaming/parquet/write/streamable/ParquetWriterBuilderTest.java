package org.sv.flexobject.hadoop.streaming.parquet.write.streamable;

import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.write.output.ByteArrayOutputFile;
import org.sv.flexobject.testdata.TestDataWithInferredSchema;

@ExtendWith(MockitoExtension.class)
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