package org.sv.flexobject;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaRegistry;

import java.util.Arrays;

import static org.junit.Assert.*;

public class StreamableWithSchemaTest extends AbstractBenchmark {

    CopyAdapter adapter = new CopyAdapter();

    @Before
    public void setUp() throws Exception {
        SchemaRegistry.getInstance().clear();
    }

    @After
    public void tearDown() throws Exception {
        SchemaRegistry.getInstance().clear();
    }

    @Test
    public void schemaRegistered() {
        assertFalse(Schema.isRegisteredSchema(SimpleTestDataWithSchema.class));
        assertNotNull(Schema.getParamNamesXref(SimpleTestDataWithSchema.class));
        assertTrue(Schema.isRegisteredSchema(SimpleTestDataWithSchema.class));
    }


    // There really is no difference as far as what is used for getters/setters
//    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @Test
    public void direct() throws Exception {
        SimpleTestDataWithSchema data = new SimpleTestDataWithSchema();
        data.int32Field = 777;

        assertEquals(777, (int)data.int32Field);
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
                "'intInMapBar':'778877'}").replace('\'', '"')).consume(testData::load);

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

        JsonNode jsonOut = JsonOutputAdapter.produce(testData::save);
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
                "'intInMapBar':'778877'}").replace('\'', '"')).consume(testData::load);

        assertEquals(777, testData.intField);
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

        JsonNode jsonOut = JsonOutputAdapter.produce(testData::save);
        String expectedJsonString = "{\"intField\":777,\"intFieldStoredAsString\":\"124567\",\"intArray\":[0,1,23232323,3,4,null,null,null,null,null],\"intList\":[0,1],\"intMap\":{\"barbar\":999,\"foofoo\":888},\"json\":{\"a\":{\"foo\":1.2345,\"bar\":true}}}";
        assertEquals(expectedJsonString, jsonOut.toString());
    }
}