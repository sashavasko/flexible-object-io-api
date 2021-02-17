package org.sv.flexobject;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ScalarFieldTyped;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDataWithInferredSchema extends StreamableWithSchema {

    public TestDataWithInferredSchema() throws NoSuchFieldException {
    }

    private int intField;
    protected Integer intFieldOptional;
    @ScalarFieldTyped(type = DataTypes.string) public int intFieldStoredAsString;
    public Integer intFieldStoredAsStringOptional;
    // Arrays must not be arrays of primitives to allow for null values
    public Integer[] intArray = new Integer[10];
    // ScalarFieldTyped is ignored for arrays, lists and maps
    @ScalarFieldTyped(type = DataTypes.string) @ValueType(type=DataTypes.int32) public List intList;
    @ValueType(type= DataTypes.int32) public Map intMap;
    public ObjectNode json;

    public static TestDataWithInferredSchema random(boolean withJson) throws Exception {
        TestDataWithInferredSchema d = new TestDataWithInferredSchema();

        d.intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        d.intFieldOptional = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        d.intFieldStoredAsString = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        for (int i = 0 ; i < 5 ; ++i )
            d.intArray[i] = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        d.intList = new ArrayList();
        for (int i = 0 ; i < 5 ; ++i )
            d.intList.add((int)Math.round(Math.random() * Integer.MAX_VALUE));
        d.intMap = new HashMap();
        for (int i = 0 ; i < 5 ; ++i )
            d.intMap.put("value"+i, (int)Math.round(Math.random() * Integer.MAX_VALUE));

        if (withJson)
            d.json = random(false).toJson();
        return d;
    }
}
