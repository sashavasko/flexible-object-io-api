package org.sv.flexobject.hadoop.streaming.testdata.levelone;

import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.testdata.levelone.leveltwo.SimpleObject;

public class ObjectWithNestedObject extends StreamableWithSchema {
    int intField;
    SimpleObject nestedObject;

    public static ObjectWithNestedObject random() {
        ObjectWithNestedObject data = new ObjectWithNestedObject();
        data.intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        data.nestedObject = SimpleObject.random();
        return data;
    }
}
