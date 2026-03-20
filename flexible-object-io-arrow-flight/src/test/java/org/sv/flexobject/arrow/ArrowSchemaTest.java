package org.sv.flexobject.arrow;

import com.carfax.dt.streaming.testdata.TestDataWithSubSchema;
import com.carfax.dt.streaming.testdata.TestDataWithSubSchemaInCollection;
import com.carfax.dt.streaming.testdata.levelone.ObjectWithNestedObject;
import com.carfax.dt.streaming.testdata.levelone.ObjectWithNestedObjectInMap;
import com.carfax.dt.streaming.testdata.levelone.leveltwo.SimpleObject;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ArrowSchemaTest {

    @Test
    public void simpleSchema() throws Exception {
        Schema arrowSchema = ArrowSchema.forSchema(com.carfax.dt.streaming.schema.Schema.getRegisteredSchema(SimpleObject.class));
        assertEquals("Schema<intField: Int(32, true)>(metadata: {name=com.carfax.dt.streaming.testdata.levelone.leveltwo.SimpleObject})", arrowSchema.toString());
    }

    @Test
    public void objectWithNestedObjectInMap() throws Exception {
        Schema arrowSchema = ArrowSchema.forSchema(com.carfax.dt.streaming.schema.Schema.getRegisteredSchema(ObjectWithNestedObjectInMap.class));
        try(BufferAllocator allocator = new RootAllocator();
            VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)){
            assertEquals("Schema<" +
                    "intField: Int(32, true), " +
                    "subStructMap: Map(false)<" +
                        "element: Struct<" +
                            "key: Utf8 not null, " +
                            "value: Struct<intField: Int(32, true)" +
                        ">" +
                    "> not null" +
                    ">" +
                    ">" +
                    "(metadata: {name=com.carfax.dt.streaming.testdata.levelone.ObjectWithNestedObjectInMap})", arrowSchema.toString());

        }
    }

    @Test
    public void testDataWithSubSchema() throws NoSuchFieldException {
        Schema arrowSchema = ArrowSchema.forSchema(com.carfax.dt.streaming.schema.Schema.getRegisteredSchema(TestDataWithSubSchema.class));
        assertEquals("Schema<" +
                        "intField: Int(32, true), " +
                        "intFieldOptional: Int(32, true), " +
                        "json: Utf8, " +
                        "subStruct: Struct<" +
                                "intField: Int(32, true), " +
                                "intFieldOptional: Int(32, true), " +
                                "intFieldStoredAsString: Utf8, " +
                                "intFieldStoredAsStringOptional: Int(32, true), " +
                                "intArray: List<element: Int(32, true) not null>, " +
                                "intList: List<element: Int(32, true) not null>, " +
                                "intMap: Map(false)<" +
                                    "element: Struct<" +
                                        "key: Utf8 not null, " +
                                        "value: Int(32, true)> not null" +
                                    ">, " +
                                "json: Utf8" +
                        ">" +
                        ">(metadata: {name=com.carfax.dt.streaming.testdata.TestDataWithSubSchema})",
                arrowSchema.toString());
    }

    @Test
    public void testDataWithSimpleSubSchema() throws Exception {
        Schema arrowSchema = ArrowSchema.forSchema(com.carfax.dt.streaming.schema.Schema.getRegisteredSchema(ObjectWithNestedObject.class));
        assertEquals("Schema<" +
                        "intField: Int(32, true), " +
                        "nestedObject: " +
                            "Struct<" +
                                "intField: Int(32, true)" +
                            ">" +
                        ">(metadata: {name=com.carfax.dt.streaming.testdata.levelone.ObjectWithNestedObject})",
                arrowSchema.toString());
    }

    @Test
    public void TestDataWithSubSchemaInCollection() throws Exception {
        Schema arrowSchema = ArrowSchema.forSchema(com.carfax.dt.streaming.schema.Schema.getRegisteredSchema(TestDataWithSubSchemaInCollection.class));
        try(BufferAllocator allocator = new RootAllocator();
            VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)){
            assertEquals("Schema<" +
                    "intField: Int(32, true), " +
                    "intFieldOptional: Int(32, true), " +
                    "json: Utf8, " +
                    "subStructArray: List<" +
                    "element: Struct<" +
                    "intField: Int(32, true), " +
                    "intFieldOptional: Int(32, true), " +
                    "intFieldStoredAsString: Utf8, " +
                    "intFieldStoredAsStringOptional: Int(32, true), " +
                    "intArray: List<" +
                    "element: Int(32, true) not null" +
                    ">, " +
                    "intList: List<" +
                    "element: Int(32, true) not null" +
                    ">, " +
                    "intMap: Map(false)<" +
                    "element: Struct<" +
                    "key: Utf8 not null, " +
                    "value: Int(32, true)" +
                    "> not null" +
                    ">, " +
                    "json: Utf8" +
                    "> not null" +
                    ">, " +
                    "subStructList: List<" +
                    "element: Struct<" +
                    "intField: Int(32, true), " +
                    "intFieldOptional: Int(32, true), " +
                    "intFieldStoredAsString: Utf8, " +
                    "intFieldStoredAsStringOptional: Int(32, true), " +
                    "intArray: List<" +
                    "element: Int(32, true) not null" +
                    ">, " +
                    "intList: List<" +
                    "element: Int(32, true) not null" +
                    ">, " +
                    "intMap: Map(false)<" +
                    "element: Struct<" +
                    "key: Utf8 not null, " +
                    "value: Int(32, true)" +
                    "> not null" +
                    ">, " +
                    "json: Utf8" +
                    "> not null" +
                    ">, " +
                    "subStructMap: Map(false)<" +
                    "element: Struct<" +
                    "key: Utf8 not null, " +
                    "value: Struct<" +
                    "intField: Int(32, true), " +
                    "intFieldOptional: Int(32, true), " +
                    "intFieldStoredAsString: Utf8, " +
                    "intFieldStoredAsStringOptional: Int(32, true), " +
                    "intArray: List<" +
                    "element: Int(32, true) not null" +
                    ">, " +
                    "intList: List<" +
                    "element: Int(32, true) not null" +
                    ">, " +
                    "intMap: Map(false)<" +
                    "element: Struct<" +
                    "key: Utf8 not null, " +
                    "value: Int(32, true)" +
                    "> not null" +
                    ">, " +
                    "json: Utf8" +
                    ">" +
                    "> not null" +
                    ">" +
                    ">(metadata: {name=com.carfax.dt.streaming.testdata.TestDataWithSubSchemaInCollection})", arrowSchema.toString());
        }
    }
}