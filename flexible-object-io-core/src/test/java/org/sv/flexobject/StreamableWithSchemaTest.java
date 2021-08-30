package org.sv.flexobject;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.schema.SchemaRegistry;
import org.sv.flexobject.testdata.*;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StreamableWithSchemaTest extends AbstractBenchmark {

    CopyAdapter adapter = new CopyAdapter();

    @Mock
    TestDataWithInferredSchema mockTestDataWithInferredSchema;

    @Before
    public void setUp() throws Exception {
        SchemaRegistry.getInstance().clear();
        when(mockTestDataWithInferredSchema.getSchema()).thenReturn(Schema.getRegisteredSchema(TestDataWithInferredSchema.class));
    }

    @After
    public void tearDown() throws Exception {
        SchemaRegistry.getInstance().clear();
    }

    @Test
    public void schemaRegistered() {
        assertTrue(Schema.isRegisteredSchema(SimpleTestDataWithSchema.class));

        Map<String,Integer> xref = Schema.getParamNamesXref(SimpleTestDataWithSchema.class);
        assertNotNull(xref);
        assertTrue(Schema.isRegisteredSchema(SimpleTestDataWithSchema.class));

        assertNotNull(Schema.getParamNamesXref(TestDataWithInferredSchema.class));
        assertTrue(Schema.isRegisteredSchema(TestDataWithInferredSchema.class));

        assertNotNull(Schema.getParamNamesXref(TestDataWithAnnotatedSchema.class));
        assertTrue(Schema.isRegisteredSchema(TestDataWithAnnotatedSchema.class));
    }


    // There really is no difference as far as what is used for getters/setters
//    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @Test
    public void direct() throws Exception {
        SimpleTestDataWithSchema data = new SimpleTestDataWithSchema();
        data.set("int32Field", 777);

        assertEquals(777, (int)data.get("int32Field"));
    }

    @Test
    public void setArray() throws Exception {
        Integer[] values = new Integer[]{234, 345, 567};
        TestDataWithInferredSchema testData = new TestDataWithInferredSchema();
        testData.set("intArray", values);
    }

    //    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    @Test
    public void fullyExplicit() throws Exception {
        SimpleTestDataWithSchema data = new SimpleTestDataWithSchema();
        data.set(SimpleTestDataWithSchema.FIELDS.int32Field, 777);

        assertEquals(777, data.get(SimpleTestDataWithSchema.FIELDS.int32Field));
    }

//    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    @Test
    public void genericSetter() throws Exception {
        SimpleTestDataWithSchema data = new SimpleTestDataWithSchema();
        data.set(SimpleTestDataWithSchema.FIELDS.int32FieldGenericSetter, 777);

        assertEquals(777, data.get(SimpleTestDataWithSchema.FIELDS.int32FieldGenericSetter));
    }

//    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    @Test
    public void genericSetterAndGetter() throws Exception {
        SimpleTestDataWithSchema data = new SimpleTestDataWithSchema();
        data.set(SimpleTestDataWithSchema.FIELDS.int32FieldGeneric, 777);

        assertEquals(777, data.get(SimpleTestDataWithSchema.FIELDS.int32FieldGeneric));
    }

//    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    @Test
    public void annotatedSchema() throws Exception {
        TestDataWithAnnotatedSchema testData = new TestDataWithAnnotatedSchema();

        testData.set(TestDataWithAnnotatedSchema.FIELDS.intField, 789);
        assertEquals(789, (int)testData.get(TestDataWithAnnotatedSchema.FIELDS.intField));

        testData.set(TestDataWithAnnotatedSchema.FIELDS.intFieldStoredAsString, 789);
        assertEquals(789, (int)testData.get(TestDataWithAnnotatedSchema.FIELDS.intFieldStoredAsString));

        JsonInputAdapter.forValue(("{'intField':777, " +
                "'intFieldStoredAsString':'124567', " +
                "'intArray':[0,1,23232323,3,4]," +  // results may be non-deterministic if values at index don't much between scalar field and array field
                "'intList':[0,1]," +
                "'intMap':{'foofoo': 888, 'barbar':'999'}," +
                "'json':{'a':{'foo':1.2345, 'bar': true}}," +
                "'intInArray2':23232323," +
                "'intInList3':2222," +
                "'intInMapFoo':565656," +
                "'intInMapBar':'778877'}").replace('\'', '"')).consume(testData);

        assertEquals(777, (int)testData.get(TestDataWithAnnotatedSchema.FIELDS.intField));
        assertEquals(124567, (int)testData.get(TestDataWithAnnotatedSchema.FIELDS.intFieldStoredAsString));
        assertEquals(565656, (int)testData.get(TestDataWithAnnotatedSchema.FIELDS.intInMapFoo));
        assertEquals(778877, (int)testData.get(TestDataWithAnnotatedSchema.FIELDS.intInMapBar));
        assertNull(testData.get(TestDataWithAnnotatedSchema.FIELDS.intInMapNull));
        assertEquals(888, (int)testData.get(TestDataWithAnnotatedSchema.FIELDS.intInMapFooFoo));
        assertEquals(999, (int)testData.get(TestDataWithAnnotatedSchema.FIELDS.intInMapBarBar));
        assertEquals(23232323, (int)testData.get(TestDataWithAnnotatedSchema.FIELDS.intInArray2));
        assertEquals(Arrays.asList(0, 1, null, 2222), testData.get(TestDataWithAnnotatedSchema.FIELDS.intList));
        int intInList3 = (int) testData.get(TestDataWithAnnotatedSchema.FIELDS.intInList3);
        assertEquals(2222, intInList3);
        assertEquals(Arrays.asList(0, 1, 23232323, 3, 4,null, null, null,null, null), Arrays.asList((Integer[])testData.get(TestDataWithAnnotatedSchema.FIELDS.intArray)));

        assertEquals(1.2345, (double)testData.get(TestDataWithAnnotatedSchema.FIELDS.doubleInJson), 0.001);
        assertTrue((boolean)testData.get(TestDataWithAnnotatedSchema.FIELDS.booleanInJson));

        JsonNode jsonOut = JsonOutputAdapter.produce(testData);
        String expectedJsonString = "{\"intField\":777,\"intFieldStoredAsString\":\"124567\",\"intInArray2\":23232323,\"intInList3\":2222,\"intInMapFoo\":565656,\"intInMapBar\":\"778877\",\"intInMapFooFoo\":888,\"intInMapBarBar\":\"999\",\"intList\":[0,1,null,2222],\"intArray\":[0,1,23232323,3,4,null,null,null,null,null],\"intMap\":{\"bar\":778877,\"foo\":565656,\"barbar\":999,\"foofoo\":888},\"doubleInJson\":1.2345,\"booleanInJson\":true,\"json\":{\"a\":{\"foo\":1.2345,\"bar\":true}}}";
        assertEquals(expectedJsonString, jsonOut.toString());
    }

    //    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
    @Test
    public void inferredSchema() throws Exception {
        TestDataWithInferredSchema testData = new TestDataWithInferredSchema();

        JsonInputAdapter.forValue(("{'intField':777, " +
                "'intFieldStoredAsString':'124567', " +
                "'intArray':[0,1,23232323,3,4]," +  // results may be non-deterministic if values at index don't much between scalar field and array field
                "'intList':[0,1]," +
                "'intMap':{'foofoo': 888, 'barbar':'999'}," +
                "'json':{'a':{'foo':1.2345, 'bar': true}}," +
                "'intInArray2':23232323," +
                "'intInList3':2222," +
                "'intInMapFoo':565656," +
                "'intInMapBar':'778877'}").replace('\'', '"')).consume(testData);

        assertEquals(777, testData.get("intField"));
        assertEquals(124567, testData.intFieldStoredAsString);
        assertNull(testData.intMap.get("foo"));
        assertNull(testData.intMap.get("bar"));
        assertEquals(888, (int)testData.intMap.get("foofoo"));
        assertEquals(999, (int)testData.intMap.get("barbar"));
        assertEquals(23232323, (int)testData.intArray[2]);
        assertEquals(Arrays.asList(0, 1), testData.intList);
        assertEquals(Arrays.asList(0, 1, 23232323, 3, 4, null, null, null,null, null), Arrays.asList(testData.intArray));

        assertEquals(1.2345, testData.json.get("a").get("foo").asDouble(), 0.001);
        assertTrue(testData.json.get("a").get("bar").asBoolean());

        JsonNode jsonOut = JsonOutputAdapter.produce(testData);
        String expectedJsonString = "{\"intField\":777,\"intFieldStoredAsString\":\"124567\",\"intArray\":[0,1,23232323,3,4,null,null,null,null,null],\"intList\":[0,1],\"intMap\":{\"barbar\":999,\"foofoo\":888},\"json\":{\"a\":{\"foo\":1.2345,\"bar\":true}}}";
        assertEquals(expectedJsonString, jsonOut.toString());
    }

    @Test
    public void classAndEnum() throws Exception {
        TestDataWithEnumAndClass testData = new TestDataWithEnumAndClass();

        JsonInputAdapter.forValue(("{'clazz':'" + TestDataWithEnumAndClass.TestEnum.class.getName() + "', " +
                "'enumValue':'uno'," +
                "'enumSet':'uno,dos'}").replace('\'', '"')).consume(testData::load);

        assertSame(TestDataWithEnumAndClass.TestEnum.class, testData.clazz);
        assertEquals(TestDataWithEnumAndClass.TestEnum.uno, testData.enumValue);
        assertEquals(EnumSet.of(TestDataWithEnumAndClass.TestEnum.uno,TestDataWithEnumAndClass.TestEnum.dos), testData.enumSet);

        JsonNode jsonOut = JsonOutputAdapter.produce(testData);
        String expectedJsonString = "{\"clazz\":\"org.sv.flexobject.testdata.TestDataWithEnumAndClass$TestEnum\",\"enumValue\":\"uno\",\"enumSet\":\"uno,dos\"}";
        assertEquals(expectedJsonString, jsonOut.toString());

        JsonInputAdapter.forValue(("{'clazz':'" + TestDataWithEnumAndClass.TestEnum.class.getName() + "', " +
                "'enumValue':'uno'," +
                "'enumSet':['uno','dos']}").replace('\'', '"')).consume(testData);

        assertEquals(EnumSet.of(TestDataWithEnumAndClass.TestEnum.uno,TestDataWithEnumAndClass.TestEnum.dos), testData.enumSet);
    }

    @Test
    public void subSchemaIsJson() throws NoSuchFieldException, SchemaException {
        Schema schema = Schema.getRegisteredSchema(TestDataWithSubSchema.class);
        assertEquals(DataTypes.jsonNode, schema.getDescriptor("subStruct").getType());
        assertEquals(TestDataWithInferredSchema.class, schema.getDescriptor("subStruct").getSubschema());
        assertNull(schema.getDescriptor("intField").getSubschema());
    }

    @Test
    public void setSubschemaFromJson() throws Exception {
        TestDataWithInferredSchema expectedSubStruct = TestDataWithInferredSchema.random(true);

        ObjectNode json = expectedSubStruct.toJson();

        TestDataWithSubSchema testData = new TestDataWithSubSchema();

        testData.set("subStruct", json);

        assertEquals(expectedSubStruct, testData.subStruct);
    }

    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    @Test
    public void clearSubschemaClears() throws Exception {
        TestDataWithSubSchema testData = new TestDataWithSubSchema();

        testData.set("subStruct", mockTestDataWithInferredSchema);

        testData.clear();

        verify(mockTestDataWithInferredSchema).clear();
    }

    @Test
    public void convertsSubschemaToJson() throws Exception {
        TestDataWithSubSchema testData = new TestDataWithSubSchema();
        testData.intField = 456;
        testData.json = (ObjectNode) MapperFactory.getObjectReader().readTree("{'foo':'bar','yes':false}".replace('\'', '"'));
        testData.subStruct = new TestDataWithInferredSchema();
        testData.subStruct.set("intField", 789);
        testData.subStruct.intMap = new HashMap();
        testData.subStruct.intMap.put("a", 111);
        testData.subStruct.intMap.put("b", 222);
        testData.subStruct.intFieldStoredAsString = 69;

        ObjectNode json = JsonOutputAdapter.produce(testData);

        assertEquals("{\"intField\":456,\"json\":{\"foo\":\"bar\",\"yes\":false},\"subStruct\":{\"intField\":789,\"intFieldStoredAsString\":\"69\",\"intArray\":[null,null,null,null,null,null,null,null,null,null],\"intMap\":{\"a\":111,\"b\":222}}}"
                , json.toString());

        TestDataWithSubSchema reloaded = new TestDataWithSubSchema();
        JsonInputAdapter.forValue(json).consume(reloaded);

        assertEquals(testData, reloaded);
    }

    @Test
    public void subSchemaInCollectionIsJson() {
        assertEquals(DataTypes.jsonNode, Schema
                .getRegisteredSchema(TestDataWithSubSchemaInCollection.class)
                .getDescriptor("subStructArray")
                .getType());
        assertEquals(DataTypes.jsonNode, Schema
                .getRegisteredSchema(TestDataWithSubSchemaInCollection.class)
                .getDescriptor("subStructList")
                .getType());
        assertEquals(DataTypes.jsonNode, Schema
                .getRegisteredSchema(TestDataWithSubSchemaInCollection.class)
                .getDescriptor("subStructMap")
                .getType());
    }

    @Test
    public void setSubschemaInArrayFromJson() throws Exception {
        List<TestDataWithInferredSchema> listOfTestValues = Arrays.asList(TestDataWithInferredSchema.random(true),
                TestDataWithInferredSchema.random(true),
                TestDataWithInferredSchema.random(true),
                TestDataWithInferredSchema.random(true),
                TestDataWithInferredSchema.random(true));

        JsonNode json = DataTypes.jsonConverter(listOfTestValues);

        TestDataWithSubSchemaInCollection testData = new TestDataWithSubSchemaInCollection();

        testData.set("subStructArray", json);

        List<TestDataWithInferredSchema> actualList = Arrays.asList(testData.subStructArray);

        assertEquals(listOfTestValues, actualList.subList(0, 5));
    }

    @Test
    public void setSubschemaInListFromJson() throws Exception {
        List<TestDataWithInferredSchema> listOfTestValues = Arrays.asList(TestDataWithInferredSchema.random(true),
                TestDataWithInferredSchema.random(true),
                TestDataWithInferredSchema.random(true));

        JsonNode json = DataTypes.jsonConverter(listOfTestValues);

        TestDataWithSubSchemaInCollection testData = new TestDataWithSubSchemaInCollection();

        testData.set("subStructList", json);

        assertEquals(3, testData.subStructList.size());
        assertEquals(listOfTestValues, testData.subStructList);
    }

    @Test
    public void setSubschemaInMapFromJson() throws Exception {
        Map<String, TestDataWithInferredSchema> expectedMap = new HashMap();

        expectedMap.put("val1", TestDataWithInferredSchema.random(true));
        expectedMap.put("val2", TestDataWithInferredSchema.random(true));
        expectedMap.put("val3", TestDataWithInferredSchema.random(true));

        JsonNode json = DataTypes.jsonConverter(expectedMap);

        TestDataWithSubSchemaInCollection testData = new TestDataWithSubSchemaInCollection();

        testData.set("subStructMap", json);

        assertEquals(expectedMap, testData.subStructMap);
    }

    @Test
    public void convertsSubschemaInCollectionToJson() throws Exception {
        TestDataWithSubSchemaInCollection testData = TestDataWithSubSchemaInCollection.random(true);

        ObjectNode json = JsonOutputAdapter.produce(testData);

        TestDataWithSubSchemaInCollection reloaded = new TestDataWithSubSchemaInCollection();
        JsonInputAdapter.forValue(json).consume(reloaded);

        assertEquals(testData, reloaded);
    }

    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    @Test
    public void clearSubschemaClearsCollections() throws Exception {
        TestDataWithSubSchemaInCollection testData = TestDataWithSubSchemaInCollection.random(true);

        testData.clear();

        assertTrue(testData.isEmpty());
    }


    @Test
    public void superclassFieldsIncluded() {
        Schema schema = Schema.getRegisteredSchema(TestDataWithSuperclass.class);

        assertEquals(11, schema.getFields().length);
    }

    @Test
    public void convertsSuperclassToJson() throws Exception {
        TestDataWithSuperclass testData = TestDataWithSuperclass.random();

        ObjectNode json = JsonOutputAdapter.produce(testData);

        System.out.println(json);
        TestDataWithSuperclass reloaded = new TestDataWithSuperclass();
        JsonInputAdapter.forValue(json).consume(reloaded);

        assertEquals(testData, reloaded);

    }
    @Test
    public void overridesPrivateAndProtectedAccess() throws Exception {
        TestDataWithSuperclass testData = TestDataWithSuperclass.random();

        testData.set("intField", 7777);
        assertEquals(7777, testData.get("intField"));
        testData.set("intFieldOptional", 777);
        assertEquals(777, testData.get("intFieldOptional"));

    }

    @Test
    public void mapWithLongKey() throws Exception {
        MapWithTypedKey testData = new MapWithTypedKey();
        MapWithTypedKey reloaded = new MapWithTypedKey();

        testData.mapWithLongKey.put(12345l, 6789l);

        JsonNode json = testData.toJson();
        System.out.println(json);

        reloaded.fromJson(json);

        assertEquals(testData, reloaded);
    }
}