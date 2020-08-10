package org.sv.flexobject.schema.reflect;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MapGetterTest {

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
        MapGetter getter = new MapGetter(TestData.class, "mapOfInts", "two");

        assertEquals(2, getter.apply(testData));
    }

    @Test
    public void applyToMapOutOfBounds() throws Exception {
        TestData testData = new TestData(testMap);
        MapGetter getter = new MapGetter(TestData.class, "mapOfInts", "five");

        assertNull(getter.apply(testData));
    }

}