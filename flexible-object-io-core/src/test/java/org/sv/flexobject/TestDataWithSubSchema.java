package org.sv.flexobject;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestDataWithSubSchema extends StreamableWithSchema {

    public TestDataWithSubSchema() throws NoSuchFieldException {
    }

    public int intField;
    public Integer intFieldOptional;
    public ObjectNode json;

    public TestDataWithInferredSchema subStruct;
}
