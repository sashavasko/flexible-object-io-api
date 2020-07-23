package org.sv.flexobject.schema.reflect;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ArrayGetterTest {

    @Test
    public void applyToArray() throws Exception {
        TestData testData = new TestData(new String[]{"one", "two", "three"});
        ArrayGetter getter = new ArrayGetter(TestData.class, "arrayOfStrings", 2);

        assertEquals("three", getter.apply(testData));
    }

    @Test
    public void applyToList() throws Exception {
        TestData testData = new TestData(Arrays.asList("one", "two", "three"));
        ArrayGetter getter = new ArrayGetter(TestData.class, "listOfStrings", 2);

        assertEquals("three", getter.apply(testData));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void applyToListOutOfBounds() throws Exception {
        TestData testData = new TestData(Arrays.asList("one", "two", "three"));
        ArrayGetter getter = new ArrayGetter(TestData.class, "listOfStrings", 3);

        getter.apply(testData);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void applyToArrayOutOfBounds() throws Exception {
        TestData testData = new TestData(new String[]{"one", "two", "three"});
        ArrayGetter getter = new ArrayGetter(TestData.class, "listOfStrings", 3);

        getter.apply(testData);
    }
}