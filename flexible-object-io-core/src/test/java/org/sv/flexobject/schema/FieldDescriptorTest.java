package org.sv.flexobject.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.schema.reflect.TestData;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

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
}