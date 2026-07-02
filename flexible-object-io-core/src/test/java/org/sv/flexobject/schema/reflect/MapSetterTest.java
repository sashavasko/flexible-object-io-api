package org.sv.flexobject.schema.reflect;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.Assert.assertThrows;

public class MapSetterTest {
    Map<String, Integer> testMap = new HashMap<>();

    @BeforeEach
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

    @Test
    public void applyToNotMap() throws Exception {
        TestData testData = new TestData(testMap);
        MapSetter setter = new MapSetter(TestData.class, "listOfStrings", "five");

        assertThrows(ClassCastException.class, ()->{setter.accept(testData, 5);});
    }


}