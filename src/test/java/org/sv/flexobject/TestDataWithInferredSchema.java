package org.sv.flexobject;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ScalarFieldTyped;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.List;
import java.util.Map;

public class TestDataWithInferredSchema extends StreamableWithSchema {

    public TestDataWithInferredSchema() throws NoSuchFieldException {
    }

    public int intField;
    public Integer intFieldOptional;
    @ScalarFieldTyped(type = DataTypes.string) public int intFieldStoredAsString;
    public Integer intFieldStoredAsStringOptional;
    // Arrays must not be arrays of primitives to allow for null values
    public Integer[] intArray = new Integer[10];
    // ScalarFieldTyped is ignored for arrays, lists and maps
    @ScalarFieldTyped(type = DataTypes.string) @ValueType(type=DataTypes.int32) public List intList;
    @ValueType(type= DataTypes.int32) public Map intMap;
    public ObjectNode json;
}
