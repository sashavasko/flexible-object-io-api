package org.sv.flexobject.schema.reflect;

import org.junit.Test;
import org.sv.flexobject.StreamableWithSchema;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class ScalarSetterTest {

    public static class BinaryTestData extends StreamableWithSchema {
        byte[] field;
    }

    @Test
    public void binaryFieldTest() throws Exception {
        BinaryTestData testData = new BinaryTestData();

        testData.set("field", "foobar".getBytes(StandardCharsets.UTF_8));

        assertEquals("foobar", new String(testData.field));
    }
}