package org.sv.flexobject.schema.reflect;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
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

    @Test
    public void applyToListOutOfBounds() throws Exception {
        TestData testData = new TestData(Arrays.asList("one", "two", "three"));
        ArrayGetter getter = new ArrayGetter(TestData.class, "listOfStrings", 3);

        assertThrows(ArrayIndexOutOfBoundsException.class, ()->{getter.apply(testData);});
    }

    @Test
    public void applyToArrayOutOfBounds() throws Exception {
        TestData testData = new TestData(new String[]{"one", "two", "three"});
        ArrayGetter getter = new ArrayGetter(TestData.class, "listOfStrings", 3);

        assertThrows(IndexOutOfBoundsException.class, ()->{getter.apply(testData);});
    }
}