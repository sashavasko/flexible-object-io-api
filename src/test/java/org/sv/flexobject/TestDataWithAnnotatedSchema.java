package org.sv.flexobject;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.*;

import java.util.List;
import java.util.Map;

public class TestDataWithAnnotatedSchema extends StreamableWithSchema {
    public enum FIELDS{
        @ScalarField intField,
        @ScalarField intFieldOptional,
        @ScalarFieldTyped(type = DataTypes.string) intFieldStoredAsString,
        @ScalarFieldTyped(type = DataTypes.string) intFieldStoredAsStringOptional,
        @ArrayField(index = 2, classFieldName = "intArray") intInArray2,
        @ArrayField(index = 3, classFieldName = "intList") intInList3,
        @MapField(type = DataTypes.int32, key = "foo", classFieldName = "intMap") intInMapFoo,
        @MapField(type = DataTypes.string, key = "bar", classFieldName = "intMap") intInMapBar,
        @MapField(type = DataTypes.string, key = "notThere", classFieldName = "intMap") intInMapNull,
        @MapField(type = DataTypes.int32, key = "foofoo", classFieldName = "intMap") intInMapFooFoo,
        @MapField(type = DataTypes.string, key = "barbar", classFieldName = "intMap") intInMapBarBar,
        @ScalarFieldTyped(type = DataTypes.jsonNode) intList,
        @ScalarFieldTyped(type = DataTypes.jsonNode) intArray,
        @ScalarFieldTyped(type = DataTypes.jsonNode) intMap,
        @JsonField(type = DataTypes.float64, path = "a.foo", classFieldName = "json") doubleInJson,
        @JsonField(type = DataTypes.bool, path = "a.bar", classFieldName = "json") booleanInJson,
        @ScalarField json,
    }

    public TestDataWithAnnotatedSchema() throws NoSuchFieldException {
        super(FIELDS.values());
    }

    int intField;
    Integer intFieldOptional;
    int intFieldStoredAsString;
    Integer intFieldStoredAsStringOptional;
    // Arrays must not be arrays of primitives to allow for null values
    Integer[] intArray = new Integer[10];
    @ValueType(type=DataTypes.int32) List intList;
    @ValueType(type=DataTypes.int32) Map intMap;
    ObjectNode json;
}
