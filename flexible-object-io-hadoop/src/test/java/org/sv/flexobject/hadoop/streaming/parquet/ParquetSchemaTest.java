package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;
import org.sv.flexobject.hadoop.streaming.parquet.read.input.ByteArrayInputFile;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.testdata.*;
import org.sv.flexobject.hadoop.streaming.parquet.write.output.ByteArrayOutputFile;
import org.sv.flexobject.hadoop.streaming.parquet.write.streamable.ParquetWriterBuilder;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.testdata.TestDataWithInferredSchema;
import org.sv.flexobject.testdata.TestDataWithInferredSchemaAndList;
import org.sv.flexobject.testdata.TestDataWithSubSchema;
import org.sv.flexobject.testdata.TestDataWithSubSchemaInCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ParquetSchemaTest {

    @Test
    public void toJsonFromJson() throws IOException {
        String jsonString = "{\"name\":\"org.sv.flexobject.hadoop.TestGroup\",\"fields\":[{\"fieldName\":\"int32Field\",\"primitive\":true,\"type\":\"INT32\"},{\"fieldName\":\"int64Field\",\"primitive\":true,\"type\":\"INT64\"},{\"fieldName\":\"booleanField\",\"primitive\":true,\"type\":\"BOOLEAN\"},{\"fieldName\":\"doubleField\",\"primitive\":true,\"type\":\"DOUBLE\"},{\"fieldName\":\"binaryField\",\"primitive\":true,\"originalType\":\"UTF8\",\"type\":\"BINARY\"},{\"fieldName\":\"int32FieldRepeated\",\"primitive\":false,\"originalType\":\"LIST\",\"fields\":[{\"fieldName\":\"list\",\"repetition\":\"REPEATED\",\"primitive\":false,\"fields\":[{\"fieldName\":\"element\",\"primitive\":true,\"type\":\"INT32\"}]}]},{\"fieldName\":\"int32FieldSimpleRepeated\",\"primitive\":false,\"originalType\":\"LIST\",\"fields\":[{\"fieldName\":\"element\",\"repetition\":\"REPEATED\",\"primitive\":true,\"type\":\"INT32\"}]},{\"fieldName\":\"subgroupField\",\"primitive\":false,\"fields\":[{\"fieldName\":\"int32Field\",\"primitive\":true,\"type\":\"INT32\"},{\"fieldName\":\"int64Field\",\"primitive\":true,\"type\":\"INT64\"}]},{\"fieldName\":\"binaryFieldRepeated\",\"primitive\":false,\"originalType\":\"LIST\",\"fields\":[{\"fieldName\":\"list\",\"repetition\":\"REPEATED\",\"primitive\":false,\"fields\":[{\"fieldName\":\"element\",\"primitive\":true,\"originalType\":\"UTF8\",\"type\":\"BINARY\"}]}]},{\"fieldName\":\"binaryFieldSimpleRepeated\",\"primitive\":false,\"originalType\":\"LIST\",\"fields\":[{\"fieldName\":\"element\",\"repetition\":\"REPEATED\",\"primitive\":true,\"originalType\":\"UTF8\",\"type\":\"BINARY\"}]},{\"fieldName\":\"longFieldRepeated\",\"primitive\":false,\"originalType\":\"LIST\",\"fields\":[{\"fieldName\":\"list\",\"repetition\":\"REPEATED\",\"primitive\":false,\"fields\":[{\"fieldName\":\"element\",\"primitive\":true,\"type\":\"INT64\"}]}]},{\"fieldName\":\"longFieldSimpleRepeated\",\"primitive\":false,\"originalType\":\"LIST\",\"fields\":[{\"fieldName\":\"element\",\"repetition\":\"REPEATED\",\"primitive\":true,\"type\":\"INT64\"}]},{\"fieldName\":\"dateField\",\"primitive\":true,\"originalType\":\"DATE\",\"type\":\"INT32\"},{\"fieldName\":\"subgroupFieldRepeated\",\"primitive\":false,\"originalType\":\"LIST\",\"fields\":[{\"fieldName\":\"list\",\"repetition\":\"REPEATED\",\"primitive\":false,\"fields\":[{\"fieldName\":\"element\",\"primitive\":false,\"fields\":[{\"fieldName\":\"int32Field\",\"primitive\":true,\"type\":\"INT32\"},{\"fieldName\":\"int64Field\",\"primitive\":true,\"type\":\"INT64\"}]}]}]},{\"fieldName\":\"subgroupFieldSimpleRepeated\",\"primitive\":false,\"originalType\":\"LIST\",\"fields\":[{\"fieldName\":\"element\",\"repetition\":\"REPEATED\",\"primitive\":false,\"fields\":[{\"fieldName\":\"int32Field\",\"primitive\":true,\"type\":\"INT32\"},{\"fieldName\":\"int64Field\",\"primitive\":true,\"type\":\"INT64\"}]}]},{\"fieldName\":\"floatField\",\"primitive\":true,\"type\":\"FLOAT\"},{\"fieldName\":\"timestamp\",\"primitive\":true,\"originalType\":\"TIMESTAMP_MILLIS\",\"type\":\"INT64\"},{\"fieldName\":\"json\",\"primitive\":true,\"originalType\":\"UTF8\",\"type\":\"BINARY\"}]}";

        MessageType parquet = ParquetSchema.fromJson((ObjectNode) MapperFactory.getObjectReader().readTree(jsonString));


        assertEquals(jsonString, ParquetSchema.toJson(parquet).toString());
    }

    @Test
    public void forTestDataWithInferredSchema() {
        MessageType parquetSchema = ParquetSchema.forClass(TestDataWithInferredSchema.class);

        assertEquals("message org.sv.flexobject.testdata.TestDataWithInferredSchema {\n" +
                "  optional int32 intField;\n" +
                "  optional int32 intFieldOptional;\n" +
                "  optional binary intFieldStoredAsString (UTF8);\n" +
                "  optional int32 intFieldStoredAsStringOptional;\n" +
                "  optional group intArray (LIST) {\n" +
                "    repeated group list {\n" +
                "      optional int32 element;\n" +
                "    }\n" +
                "  }\n" +
                "  optional group intList (LIST) {\n" +
                "    repeated group list {\n" +
                "      optional int32 element;\n" +
                "    }\n" +
                "  }\n" +
                "  optional group intMap (MAP) {\n" +
                "    repeated group key_value {\n" +
                "      required binary key (UTF8);\n" +
                "      optional int32 value;\n" +
                "    }\n" +
                "  }\n" +
                "  optional binary json (JSON);\n" +
                "}\n", parquetSchema.toString());
    }

    @Test
    public void forTestDataWithSubSchema() {
        MessageType parquetSchema = ParquetSchema.forClass(TestDataWithSubSchema.class);

        assertEquals("message org.sv.flexobject.testdata.TestDataWithSubSchema {\n" +
                "  optional int32 intField;\n" +
                "  optional int32 intFieldOptional;\n" +
                "  optional binary json (JSON);\n" +
                "  optional group subStruct {\n" +
                "    optional int32 intField;\n" +
                "    optional int32 intFieldOptional;\n" +
                "    optional binary intFieldStoredAsString (UTF8);\n" +
                "    optional int32 intFieldStoredAsStringOptional;\n" +
                "    optional group intArray (LIST) {\n" +
                "      repeated group list {\n" +
                "        optional int32 element;\n" +
                "      }\n" +
                "    }\n" +
                "    optional group intList (LIST) {\n" +
                "      repeated group list {\n" +
                "        optional int32 element;\n" +
                "      }\n" +
                "    }\n" +
                "    optional group intMap (MAP) {\n" +
                "      repeated group key_value {\n" +
                "        required binary key (UTF8);\n" +
                "        optional int32 value;\n" +
                "      }\n" +
                "    }\n" +
                "    optional binary json (JSON);\n" +
                "  }\n" +
                "}\n", parquetSchema.toString());
    }

    @Test
    public void forListOfBinaries() {
        MessageType parquetSchema = ParquetSchema.forClass(ListOfBinaries.class);

        assertEquals("message org.sv.flexobject.hadoop.streaming.parquet.testdata.ListOfBinaries {\n" +
                "  optional group binaryFieldRepeated (LIST) {\n" +
                "    repeated group list {\n" +
                "      optional binary element;\n" +
                "    }\n" +
                "  }\n" +
                "}\n", parquetSchema.toString());
    }

    @Test
    public void forListOfStrings() {
        MessageType parquetSchema = ParquetSchema.forClass(ListOfStrings.class);

        assertEquals("message org.sv.flexobject.hadoop.streaming.parquet.testdata.ListOfStrings {\n" +
                "  optional group binaryFieldRepeated (LIST) {\n" +
                "    repeated group list {\n" +
                "      optional binary element (UTF8);\n" +
                "    }\n" +
                "  }\n" +
                "  optional group binaryFieldSimpleRepeated (LIST) {\n" +
                "    repeated group list {\n" +
                "      optional binary element (UTF8);\n" +
                "    }\n" +
                "  }\n" +
                "}\n", parquetSchema.toString());
    }

    @Test
    public void forTestDataWithSubSchemaInCollection() {
        MessageType parquetSchema = ParquetSchema.forClass(TestDataWithSubSchemaInCollection.class);

        assertEquals("message org.sv.flexobject.testdata.TestDataWithSubSchemaInCollection {\n" +
                "  optional int32 intField;\n" +
                "  optional int32 intFieldOptional;\n" +
                "  optional binary json (JSON);\n" +
                "  optional group subStructArray (LIST) {\n" +
                "    repeated group list {\n" +
                "      optional group element {\n" +
                "        optional int32 intField;\n" +
                "        optional int32 intFieldOptional;\n" +
                "        optional binary intFieldStoredAsString (UTF8);\n" +
                "        optional int32 intFieldStoredAsStringOptional;\n" +
                "        optional group intArray (LIST) {\n" +
                "          repeated group list {\n" +
                "            optional int32 element;\n" +
                "          }\n" +
                "        }\n" +
                "        optional group intList (LIST) {\n" +
                "          repeated group list {\n" +
                "            optional int32 element;\n" +
                "          }\n" +
                "        }\n" +
                "        optional group intMap (MAP) {\n" +
                "          repeated group key_value {\n" +
                "            required binary key (UTF8);\n" +
                "            optional int32 value;\n" +
                "          }\n" +
                "        }\n" +
                "        optional binary json (JSON);\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  optional group subStructList (LIST) {\n" +
                "    repeated group list {\n" +
                "      optional group element {\n" +
                "        optional int32 intField;\n" +
                "        optional int32 intFieldOptional;\n" +
                "        optional binary intFieldStoredAsString (UTF8);\n" +
                "        optional int32 intFieldStoredAsStringOptional;\n" +
                "        optional group intArray (LIST) {\n" +
                "          repeated group list {\n" +
                "            optional int32 element;\n" +
                "          }\n" +
                "        }\n" +
                "        optional group intList (LIST) {\n" +
                "          repeated group list {\n" +
                "            optional int32 element;\n" +
                "          }\n" +
                "        }\n" +
                "        optional group intMap (MAP) {\n" +
                "          repeated group key_value {\n" +
                "            required binary key (UTF8);\n" +
                "            optional int32 value;\n" +
                "          }\n" +
                "        }\n" +
                "        optional binary json (JSON);\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  optional group subStructMap (MAP) {\n" +
                "    repeated group key_value {\n" +
                "      required binary key (UTF8);\n" +
                "      optional group value {\n" +
                "        optional int32 intField;\n" +
                "        optional int32 intFieldOptional;\n" +
                "        optional binary intFieldStoredAsString (UTF8);\n" +
                "        optional int32 intFieldStoredAsStringOptional;\n" +
                "        optional group intArray (LIST) {\n" +
                "          repeated group list {\n" +
                "            optional int32 element;\n" +
                "          }\n" +
                "        }\n" +
                "        optional group intList (LIST) {\n" +
                "          repeated group list {\n" +
                "            optional int32 element;\n" +
                "          }\n" +
                "        }\n" +
                "        optional group intMap (MAP) {\n" +
                "          repeated group key_value {\n" +
                "            required binary key (UTF8);\n" +
                "            optional int32 value;\n" +
                "          }\n" +
                "        }\n" +
                "        optional binary json (JSON);\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n", parquetSchema.toString());
    }


    @Test
    public void readWriteTestDataWithInferredSchemaList() throws Exception {
        ByteArrayOutputFile outputFile = new ByteArrayOutputFile();
        TestDataWithInferredSchemaAndList data = TestDataWithInferredSchemaAndList.random(false);
        try(ParquetWriter<TestDataWithInferredSchemaAndList> writer = ParquetWriterBuilder.forOutput(outputFile).withSchema(TestDataWithInferredSchemaAndList.class).build()){
            writer.write(data);
        }

        TestDataWithInferredSchemaAndList convertedData;
        try(ParquetReader<TestDataWithInferredSchemaAndList> reader = ParquetReaderBuilder.forInput(new ByteArrayInputFile(outputFile.toByteArray())).withSchema(TestDataWithInferredSchemaAndList.class).build()){
            convertedData = reader.read();
        }

        assertEquals(data, convertedData);
    }

    @Test
    public void readWriteTestDataWithInferredSchema() throws Exception {
        ByteArrayOutputFile outputFile = new ByteArrayOutputFile();
        TestDataWithInferredSchema data = TestDataWithInferredSchema.random(false);
        try(ParquetWriter<TestDataWithInferredSchema> writer = ParquetWriterBuilder.forOutput(outputFile).withSchema(TestDataWithInferredSchema.class).build()){
            writer.write(data);
        }

        TestDataWithInferredSchema convertedData;
        try(ParquetReader<TestDataWithInferredSchema> reader = ParquetReaderBuilder.forInput(new ByteArrayInputFile(outputFile.toByteArray())).withSchema(TestDataWithInferredSchema.class).build()){
            convertedData = reader.read();
        }

        assertEquals(data, convertedData);
    }

    @Test
    public void readWriteTestDataWithListOfObject() throws Exception {
        ByteArrayOutputFile outputFile = new ByteArrayOutputFile();
        StreamableWithListOfObjects data = StreamableWithListOfObjects.random();
        try(ParquetWriter<StreamableWithListOfObjects> writer = ParquetWriterBuilder.forOutput(outputFile).withSchema(StreamableWithListOfObjects.class).build()){
            writer.write(data);
        }

        StreamableWithListOfObjects convertedData;
        try(ParquetReader<StreamableWithListOfObjects> reader = ParquetReaderBuilder.forInput(new ByteArrayInputFile(outputFile.toByteArray())).withSchema(StreamableWithListOfObjects.class).build()){
            convertedData = reader.read();
        }
        assertEquals(data, convertedData);
    }

    @Test
    public void readWriteTestDataSubSchema() throws Exception {
        ByteArrayOutputFile outputFile = new ByteArrayOutputFile();
        StreamableWithNonRepeatedSubschema data = StreamableWithNonRepeatedSubschema.random();
        try(ParquetWriter<StreamableWithNonRepeatedSubschema> writer = ParquetWriterBuilder
                .forOutput(outputFile)
                .withSchema(StreamableWithNonRepeatedSubschema.class).build()){
            writer.write(data);
        }

        StreamableWithNonRepeatedSubschema convertedData;
        try(ParquetReader<StreamableWithNonRepeatedSubschema> reader = ParquetReaderBuilder
                .forInput(new ByteArrayInputFile(outputFile.toByteArray()))
                .withSchema(StreamableWithNonRepeatedSubschema.class).build()){
            convertedData = reader.read();
        }
        assertEquals(data, convertedData);
    }

    @Test
    public void readWriteTestDataWithListOfObjectSeveral() throws Exception {
        ByteArrayOutputFile outputFile = new ByteArrayOutputFile();
        List<StreamableWithListOfObjects> originalData = new ArrayList<>();
        try(ParquetWriter<StreamableWithListOfObjects> writer = ParquetWriterBuilder.forOutput(outputFile).withSchema(StreamableWithListOfObjects.class).build()){
            for (int i = 0 ; i < 10 ; ++i) {
                StreamableWithListOfObjects data = StreamableWithListOfObjects.random();
                writer.write(data);
                originalData.add(data);
            }
        }

        List<StreamableWithListOfObjects> convertedList = new ArrayList<>();
        try(ParquetReader<StreamableWithListOfObjects> reader = ParquetReaderBuilder.forInput(new ByteArrayInputFile(outputFile.toByteArray())).withSchema(StreamableWithListOfObjects.class).build()){
            StreamableWithListOfObjects convertedData;
            do {
                convertedData = reader.read();
                if (convertedData != null)
                    convertedList.add(convertedData);
            } while (convertedData != null);
        }
        assertEquals(originalData, convertedList);
    }

    @Test
    public void readWriteTestDataWithMapOfObject() throws Exception {
        ByteArrayOutputFile outputFile = new ByteArrayOutputFile();
        StreamableWithMapOfObjects data = StreamableWithMapOfObjects.random();
        try(ParquetWriter<StreamableWithMapOfObjects> writer = ParquetWriterBuilder.forOutput(outputFile).withSchema(StreamableWithMapOfObjects.class).build()){
            writer.write(data);
        }

        StreamableWithMapOfObjects convertedData;
        try(ParquetReader<StreamableWithMapOfObjects> reader = ParquetReaderBuilder.forInput(new ByteArrayInputFile(outputFile.toByteArray())).withSchema(StreamableWithMapOfObjects.class).build()){
            convertedData = reader.read();
        }
        assertEquals(data, convertedData);
    }

}