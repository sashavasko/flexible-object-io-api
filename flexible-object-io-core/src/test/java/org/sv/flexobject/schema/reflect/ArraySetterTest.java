package org.sv.flexobject.schema.reflect;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ArraySetterTest {

    @Test
    public void applyToArray() throws Exception {
        String[] testArray = new String[]{"one", "two", "three"};
        TestData testData = new TestData(testArray);
        ArraySetter setter = new ArraySetter(TestData.class, "arrayOfStrings", 2);

        setter.accept(testData, "foobar");

        assertEquals("foobar", testArray[2]);
    }

    @Test
    public void applyToList() throws Exception {
        List<String> testList = Arrays.asList("one", "two", "three");
        TestData testData = new TestData(testList);
        ArraySetter setter = new ArraySetter(TestData.class, "listOfStrings", 2);

        setter.accept(testData, "foobar");

        assertEquals("foobar", testList.get(2));
    }


}