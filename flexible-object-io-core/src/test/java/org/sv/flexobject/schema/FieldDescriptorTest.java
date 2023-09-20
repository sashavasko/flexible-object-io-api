package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.schema.annotations.FieldName;
import org.sv.flexobject.schema.reflect.TestData;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FieldDescriptorTest {

    CopyAdapter adapter = new CopyAdapter();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        adapter.clear();
    }

    @Test
    public void builderForArrayField() throws Exception {
        FieldDescriptor fd = FieldDescriptor.builder()
                .withClass(TestData.class)
                .withName("stringNo2")
                .withClassFieldName("arrayOfStrings")
                .withType(DataTypes.string)
                .asArrayElement(1).build();
        TestData testData = new TestData(new String[]{"zero", "one", "two","three"});

        assertEquals("one", fd.get(testData));

        fd.set(testData, "foo");

        assertEquals("foo", testData.getArrayOfStrings()[1]);

        fd.save(testData, adapter);

        assertEquals("foo", adapter.get("stringNo2"));

        adapter.put("stringNo2", "bar");

        fd.load(testData, adapter);

        assertEquals("bar", testData.getArrayOfStrings()[1]);
    }

    @Test
    public void builderForSetField() throws Exception {
        FieldDescriptor fd = FieldDescriptor.builder()
                .withClass(TestData.class)
                .withName("stringNo2")
                .withClassFieldName("setOfStrings")
                .withType(DataTypes.string)
                .build();
        TestData testData = new TestData(new HashSet<>(Arrays.asList("zero", "one", "two", "three")));

        assertEquals(new HashSet<>(Arrays.asList("zero", "one", "two", "three")), fd.get(testData));

        fd.set(testData, Arrays.asList("foo", "bar"));

        assertTrue(testData.getSetOfStrings().contains("foo"));

        fd.save(testData, adapter);

        assertEquals("[\"bar\",\"foo\"]", adapter.get("stringNo2"));

//        adapter.put("stringNo2", "bar");

        fd.load(testData, adapter);

        assertTrue(testData.getSetOfStrings().contains("bar"));
    }
    @Test
    public void builderForListField() throws Exception {
        FieldDescriptor fd = FieldDescriptor.builder()
                .withClass(TestData.class)
                .withName("stringNo2")
                .withClassFieldName("listOfStrings")
                .withType(DataTypes.string)
                .build();
        TestData testData = new TestData(Arrays.asList("zero", "one", "two", "three"));

        assertEquals(Arrays.asList("zero", "one", "two", "three"), fd.get(testData));

        fd.set(testData, Arrays.asList("foo", "bar"));

        assertTrue(testData.getListOfStrings().contains("foo"));

        fd.save(testData, adapter);

        assertEquals("[\"foo\",\"bar\"]", adapter.get("stringNo2"));

        adapter.put("stringNo2", "bar");

        fd.load(testData, adapter);

        assertTrue(testData.getListOfStrings().contains("bar"));
    }

    @Test
    public void builderForMapField() throws Exception {

        FieldDescriptor fd = FieldDescriptor.builder()
                .withClass(TestData.class)
                .withName("intForFox")
                .withClassFieldName("mapOfInts")
                .withType(DataTypes.int32)
                .asMapEntry("fox").build();
        TestData testData = new TestData(new HashMap<>());

        fd.set(testData, 999);

        assertEquals(999, (int)testData.getMapOfInts().get("fox"));
        assertEquals(999, (int)fd.get(testData));

        fd.save(testData, adapter);

        assertEquals(999, adapter.get("intForFox"));

        adapter.put("intForFox", 777);

        fd.load(testData, adapter);

        assertEquals(777, (int)testData.getMapOfInts().get("fox"));
    }

    @Test
    public void builderForJsonField() throws Exception {

        FieldDescriptor fd = FieldDescriptor.builder()
                .withClass(TestData.class)
                .withName("intForFox")
                .withClassFieldName("json")
                .withType(DataTypes.int32)
                .asJson("a.b.fox").build();
        TestData testData = new TestData();

        fd.set(testData, 999);

        assertEquals(999, testData.getJson().get("a").get("b").get("fox").asInt());
        assertEquals(999, (int)fd.get(testData));

        fd.save(testData, adapter);

        assertEquals(999, adapter.get("intForFox"));

        adapter.put("intForFox", 777);

        fd.load(testData, adapter);

        assertEquals(777, testData.getJson().get("a").get("b").get("fox").asInt());
    }

    public static class TestDataWithNamedField extends StreamableImpl {
        @FieldName(name = "@type")
        public String type;
    }

    @Test
    public void fieldName() throws Exception {
        TestDataWithNamedField testData = new TestDataWithNamedField();
        testData.type = "fooBar";

        ObjectNode json = testData.toJson();
        assertEquals("fooBar", json.get("@type").asText());

        TestDataWithNamedField testData2 = new TestDataWithNamedField();
        testData2.fromJson(json);

        assertEquals("fooBar", testData2.type);

    }

}