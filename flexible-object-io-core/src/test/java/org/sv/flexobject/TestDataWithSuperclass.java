package org.sv.flexobject;

import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.schema.annotations.NonStreamableField;

public class TestDataWithSuperclass extends TestDataWithInferredSchema {

    public int myInt;
    public Long myLong;
    public String myString;
    @NonStreamableField public int nonStreamableInt;

    public TestDataWithSuperclass() throws NoSuchFieldException, SchemaException {
    }

    public static TestDataWithSuperclass random() throws Exception {
        TestDataWithSuperclass data = new TestDataWithSuperclass();

        data.fromJson(TestDataWithInferredSchema.random(true).toJson());

        data.myInt = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        data.myLong = (long)Math.round(Math.random() * Long.MAX_VALUE);
        data.myString = "my values are " + data.myInt + " and " + data.myLong;
        return data;
    }

}
