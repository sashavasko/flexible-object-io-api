package org.sv.flexobject.schema;

import org.junit.Test;
import org.sv.flexobject.StreamableImpl;

import static org.junit.Assert.assertNotNull;

public class SchemaTest {

    public static class TestData extends StreamableImpl {
        int foo;
    }

    @Test
    public void getParamNamesXref() {
        assertNotNull(Schema.getParamNamesXref(TestData.class));
    }
}