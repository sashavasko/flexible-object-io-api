package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReadSupport;
import org.sv.flexobject.testdata.SimpleTestDataWithSchema;
import org.sv.flexobject.testdata.TestDataWithSubSchema;

import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class ParquetReadSupportTest {

    @Test
    public void testSimpleTestDataWithSchema() {
        ParquetReadSupport readSupport = new ParquetReadSupport();
        Configuration conf = new Configuration();
        MessageType parquetSchema = ParquetSchema.forClass(SimpleTestDataWithSchema.class);

        RecordMaterializer rm = readSupport.prepareForRead(conf, null, parquetSchema, new ReadSupport.ReadContext(parquetSchema));

        assertNotNull(rm);
    }

    @Test
    public void testTestDataWithSubSchema() {
        ParquetReadSupport readSupport = new ParquetReadSupport();
        Configuration conf = new Configuration();
        MessageType parquetSchema = ParquetSchema.forClass(TestDataWithSubSchema.class);

        RecordMaterializer rm = readSupport.prepareForRead(conf, null, parquetSchema, new ReadSupport.ReadContext(parquetSchema));

        assertNotNull(rm);
    }
}