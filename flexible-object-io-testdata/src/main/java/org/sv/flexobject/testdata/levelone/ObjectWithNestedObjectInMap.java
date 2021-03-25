package org.sv.flexobject.testdata.levelone;

import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.annotations.ValueClass;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;

import java.util.HashMap;
import java.util.Map;

public class ObjectWithNestedObjectInMap extends StreamableWithSchema {
    int intField;
    @ValueClass(valueClass = SimpleObject.class)
    Map<String, SimpleObject> subStructMap = new HashMap<>();

    public static ObjectWithNestedObjectInMap random() {
        ObjectWithNestedObjectInMap data = new ObjectWithNestedObjectInMap();
        data.intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        data.subStructMap.put("key1", SimpleObject.random());
        data.subStructMap.put("key2", SimpleObject.random());
        data.subStructMap.put("key3", SimpleObject.random());
        data.subStructMap.put("key4", SimpleObject.random());
        return data;
    }
}
