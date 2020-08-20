package org.sv.flexobject;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDataWithSubSchemaInCollection extends StreamableWithSchema {

    public TestDataWithSubSchemaInCollection() throws NoSuchFieldException {
    }

    public int intField;
    public Integer intFieldOptional;
    public ObjectNode json;

    @ValueClass(valueClass = TestDataWithInferredSchema.class)
    public TestDataWithInferredSchema[] subStructArray = new TestDataWithInferredSchema[10];
    @ValueClass(valueClass = TestDataWithInferredSchema.class)
    public List subStructList;
    @ValueClass(valueClass = TestDataWithInferredSchema.class)
    public Map subStructMap;

    public static TestDataWithSubSchemaInCollection random(boolean withJson) throws Exception {
        TestDataWithSubSchemaInCollection d = new TestDataWithSubSchemaInCollection();
        d.intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        d.intFieldOptional = (int)Math.round(Math.random() * Integer.MAX_VALUE);

        if (withJson){
            d.json = random(false).toJson();
        }
        for (int i = 0 ; i < 5 ; ++i )
            d.subStructArray[i] = TestDataWithInferredSchema.random(true);
        d.subStructList = new ArrayList();
        for (int i = 0 ; i < 5 ; ++i )
            d.subStructList.add(TestDataWithInferredSchema.random(true));
        d.subStructMap = new HashMap();
        for (int i = 0 ; i < 5 ; ++i )
            d.subStructMap.put("value"+i, TestDataWithInferredSchema.random(true));

        return d;
    }
}
