package org.sv.flexobject.schema.reflect;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapSetterTest {
    Map<String, Integer> testMap = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        testMap.put("one", 1);
        testMap.put("two", 2);
        testMap.put("three", 3);
    }

    @Test
    public void applyToMap() throws Exception {
        TestData testData = new TestData(testMap);
        MapSetter setter = new MapSetter(TestData.class, "mapOfInts", "two");

        setter.accept(testData, 7);

        assertEquals(7, (int)testMap.get("two"));
    }

    @Test
    public void applyToMapNewItem() throws Exception {
        TestData testData = new TestData(testMap);
        MapSetter setter = new MapSetter(TestData.class, "mapOfInts", "five");

        setter.accept(testData, 5);

        assertEquals(5, (int)testMap.get("five"));
    }

    @Test(expected = ClassCastException.class)
    public void applyToNotMap() throws Exception {
        TestData testData = new TestData(testMap);
        MapSetter setter = new MapSetter(TestData.class, "listOfStrings", "five");

        setter.accept(testData, 5);
    }


}