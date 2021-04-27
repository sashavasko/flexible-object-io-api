package org.sv.flexobject.testdata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ScalarFieldTyped;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.List;

public class TestDataWithInferredSchemaAndList extends StreamableWithSchema {

    public TestDataWithInferredSchemaAndList() throws NoSuchFieldException {
    }

    public int intField;
    public Integer intFieldOptional;
    @ScalarFieldTyped(type = DataTypes.string) public int intFieldStoredAsString;
    public Integer intFieldStoredAsStringOptional;
    // Arrays must not be arrays of primitives to allow for null values
    public Integer[] intArray = new Integer[10];
    // ScalarFieldTyped is ignored for arrays, lists and maps
    @ScalarFieldTyped(type = DataTypes.string) @ValueType(type=DataTypes.int32) public List intList;
    public ObjectNode json;

    public static TestDataWithInferredSchemaAndList random(boolean withJson) throws Exception {
        TestDataWithInferredSchemaAndList d = new TestDataWithInferredSchemaAndList();

        d.intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        d.intFieldOptional = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        d.intFieldStoredAsString = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        for (int i = 0 ; i < 5 ; ++i )
            d.intArray[i] = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        d.intList = new ArrayList();
        for (int i = 0 ; i < 5 ; ++i )
            d.intList.add((int)Math.round(Math.random() * Integer.MAX_VALUE));

        if (withJson)
            d.json = random(false).toJson();
        return d;
    }
}
