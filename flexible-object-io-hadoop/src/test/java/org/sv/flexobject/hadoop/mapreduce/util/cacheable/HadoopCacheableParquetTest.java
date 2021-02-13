package org.sv.flexobject.hadoop.mapreduce.util.cacheable;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.After;
import org.junit.Test;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.input.ByteArrayInputFile;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.write.output.ByteArrayOutputFile;
import org.sv.flexobject.hadoop.streaming.parquet.write.streamable.ParquetWriterBuilder;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HadoopCacheableParquetTest {

    public static final Path DIR = new Path("/blah");
    public static final String COUNTER = "Blah: ";

    Map<String, String> keyValue = new HashMap<>();
    ByteArrayOutputFile testData = new ByteArrayOutputFile();

    public static class TestSchema extends StreamableWithSchema {
        String key;
        String value;

        public TestSchema() {
        }

        public TestSchema(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    HadoopCacheableParquet cacheable = new HadoopCacheableParquet<TestSchema>(DIR, COUNTER, "test", TestSchema.class) {
        @Override
        protected void addItem(TestSchema item) {
            keyValue.put(item.key, item.value);
        }

        @Override
        public void clear() {
            keyValue.clear();
        }

        @Override
        protected ParquetReader.Builder getBuilder(String filename, String path) {
            return ParquetReaderBuilder.forInput(new ByteArrayInputFile(testData.toByteArray())).withSchema(TestSchema.class);
        }
    };

    @After
    public void tearDown() throws Exception {
        keyValue.clear();
    }

    @Test
    public void testFileParse() throws Exception {

        try(ParquetWriter<TestSchema> writer = ParquetWriterBuilder.forOutput(testData).withSchema(TestSchema.class).build()){
            writer.write(new TestSchema("key1", "value1"));
            writer.write(new TestSchema("key2", "value2"));
        }

        cacheable.loadCachedFile("test.parquet", "test.parquet");

        assertEquals("value1", keyValue.get("key1"));
        assertEquals("value2", keyValue.get("key2"));
    }

    @Test
    public void add() {
        cacheable.add("foo\tbar");

        assertEquals("bar", keyValue.get("foo"));
    }
}