package org.sv.flexobject.hadoop.streaming;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.StreamableWithSchema;

public class TestDataWithSubSchema extends StreamableWithSchema {

    public TestDataWithSubSchema() throws NoSuchFieldException {
    }

    public int intField;
    public Integer intFieldOptional;
    public ObjectNode json;

    public TestDataWithInferredSchema subStruct;
}
