package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.testdata.TestDataWithInferredSchema;
import org.sv.flexobject.testdata.TestDataWithSubSchema;
import org.sv.flexobject.testdata.TestDataWithSubSchemaInCollection;

import java.io.IOException;

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
}