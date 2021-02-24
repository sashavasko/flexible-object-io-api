package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;
import org.sv.flexobject.hadoop.mapreduce.input.parquet.JsonParquetInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.parquet.StreamableParquetInputFormat;
import org.sv.flexobject.hadoop.mapreduce.output.parquet.JsonParquetOutputFormat;
import org.sv.flexobject.hadoop.mapreduce.output.parquet.StreamableParquetOutputFormat;
import org.sv.flexobject.hadoop.streaming.testdata.TestDataWithSubSchema;
import org.sv.flexobject.hadoop.streaming.testdata.TestDataWithSubSchemaInCollection;

import static org.junit.Assert.*;

public class ParquetSchemaConfTest {

    @Test
    public void writesToConfigurationClasses() throws Exception {
        ParquetSchemaConf parquetConf = new ParquetSchemaConf();

        assertFalse(parquetConf.hasInputSchema());
        assertFalse(parquetConf.hasOutputSchema());
        assertSame(JsonParquetInputFormat.class, parquetConf.getInputFormat());
        assertSame(JsonParquetOutputFormat.class, parquetConf.getOutputFormat());
        assertSame(JsonNode.class, parquetConf.getOutputClass());

        parquetConf.setInputSchemaClass(TestDataWithSubSchema.class);

        assertTrue(parquetConf.hasInputSchema());
        assertFalse(parquetConf.hasOutputSchema());

        parquetConf.setOutputSchemaClass(TestDataWithSubSchemaInCollection.class);

        assertTrue(parquetConf.hasInputSchema());
        assertTrue(parquetConf.hasOutputSchema());

        assertSame(StreamableParquetInputFormat.class, parquetConf.getInputFormat());
        assertSame(StreamableParquetOutputFormat.class, parquetConf.getOutputFormat());
        assertSame(TestDataWithSubSchemaInCollection.class, parquetConf.getOutputClass());

        Configuration conf = new Configuration(false);

        parquetConf.update(conf);

        conf.writeXml(System.out);

        assertEquals(TestDataWithSubSchema.class.getName(), conf.get("org.sv.flexobject.parquet.input.schema.class"));
        assertEquals(TestDataWithSubSchemaInCollection.class.getName(), conf.get("org.sv.flexobject.parquet.output.schema.class"));
        assertNull(conf.get("org.sv.flexobject.parquet.input.schema.json"));
        assertNull(conf.get("org.sv.flexobject.parquet.output.schema.json"));

        ParquetSchemaConf parquetConf2 = new ParquetSchemaConf().from(conf);

        MessageType inputSchema = ParquetSchema.forClass(TestDataWithSubSchema.class);
        MessageType outputSchema = ParquetSchema.forClass(TestDataWithSubSchemaInCollection.class);
        assertEquals(inputSchema, parquetConf2.getInputSchema());
        assertEquals(outputSchema, parquetConf2.getOutputSchema());

        assertTrue(parquetConf2.hasInputSchema());
        assertTrue(parquetConf2.hasOutputSchema());

        assertSame(StreamableParquetInputFormat.class, parquetConf2.getInputFormat());
        assertSame(StreamableParquetOutputFormat.class, parquetConf2.getOutputFormat());
        assertSame(TestDataWithSubSchemaInCollection.class, parquetConf2.getOutputClass());

    }

    @Test
    public void writesToConfigurationJson() throws Exception {
        ParquetSchemaConf parquetConf = new ParquetSchemaConf();
        MessageType inputSchema = ParquetSchema.forClass(TestDataWithSubSchema.class);
        JsonNode inputSchemaJson = ParquetSchema.toJson(inputSchema);
        MessageType outputSchema = ParquetSchema.forClass(TestDataWithSubSchemaInCollection.class);
        JsonNode outputSchemaJson = ParquetSchema.toJson(outputSchema);

        assertFalse(parquetConf.hasInputSchema());
        assertFalse(parquetConf.hasOutputSchema());

        parquetConf.setInputSchemaJson(inputSchemaJson);

        assertTrue(parquetConf.hasInputSchema());
        assertFalse(parquetConf.hasOutputSchema());

        parquetConf.setOutputSchemaJson(outputSchemaJson);

        assertTrue(parquetConf.hasInputSchema());
        assertTrue(parquetConf.hasOutputSchema());

        assertSame(JsonParquetInputFormat.class, parquetConf.getInputFormat());
        assertSame(JsonParquetOutputFormat.class, parquetConf.getOutputFormat());
        assertSame(JsonNode.class, parquetConf.getOutputClass());

        Configuration conf = new Configuration(false);

        parquetConf.update(conf);

        conf.writeXml(System.out);

        assertNull(conf.get("org.sv.flexobject.parquet.input.schema.class"));
        assertNull(conf.get("org.sv.flexobject.parquet.output.schema.class"));
        assertEquals(inputSchemaJson.toString(), conf.get("org.sv.flexobject.parquet.input.schema.json"));
        assertEquals(outputSchemaJson.toString(), conf.get("org.sv.flexobject.parquet.output.schema.json"));

        ParquetSchemaConf parquetConf2 = new ParquetSchemaConf().from(conf);

        assertEquals(inputSchema, parquetConf2.getInputSchema());
        assertEquals(outputSchema, parquetConf2.getOutputSchema());

        assertSame(JsonParquetInputFormat.class, parquetConf.getInputFormat());
        assertSame(JsonParquetOutputFormat.class, parquetConf.getOutputFormat());
        assertSame(JsonNode.class, parquetConf.getOutputClass());

    }

    @Test
    public void overwritesToConfigurationJson() throws Exception {
        ParquetSchemaConf parquetConf = new ParquetSchemaConf();
        MessageType inputSchema = ParquetSchema.forClass(TestDataWithSubSchema.class);
        JsonNode inputSchemaJson = ParquetSchema.toJson(inputSchema);
        MessageType outputSchema = ParquetSchema.forClass(TestDataWithSubSchemaInCollection.class);
        JsonNode outputSchemaJson = ParquetSchema.toJson(outputSchema);
        parquetConf.setInputSchemaJson(inputSchemaJson);
        parquetConf.setOutputSchemaJson(outputSchemaJson);

        Configuration conf = new Configuration(false);

        conf.set("dont.touch.that", "foobar");

        parquetConf.update(conf);

        assertEquals("foobar", conf.get("dont.touch.that"));

        ParquetSchemaConf parquetConf2 = new ParquetSchemaConf();

        parquetConf2.setInputSchemaClass(TestDataWithSubSchema.class);
        parquetConf2.setOutputSchemaClass(TestDataWithSubSchemaInCollection.class);

        parquetConf2.update(conf);

        conf.writeXml(System.out);

        assertEquals(TestDataWithSubSchema.class.getName(), conf.get("org.sv.flexobject.parquet.input.schema.class"));
        assertEquals(TestDataWithSubSchemaInCollection.class.getName(), conf.get("org.sv.flexobject.parquet.output.schema.class"));
        assertNull(conf.get("org.sv.flexobject.parquet.input.schema.json"));
        assertNull(conf.get("org.sv.flexobject.parquet.output.schema.json"));

        assertEquals("foobar", conf.get("dont.touch.that"));
    }

}