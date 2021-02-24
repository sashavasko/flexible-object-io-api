package org.sv.flexobject.hadoop.streaming.testdata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.StreamableWithSchema;

public class TestDataWithSubSchema extends StreamableWithSchema {

    public TestDataWithSubSchema() throws NoSuchFieldException {
    }

    public int intField;
    public Integer intFieldOptional;
    public ObjectNode json;
    public TestDataWithInferredSchema subStruct;

    public static TestDataWithSubSchema random(boolean withJson) throws Exception {
        TestDataWithSubSchema data = new TestDataWithSubSchema();
        data.intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        data.intFieldOptional = (int)Math.round(Math.random() * Integer.MAX_VALUE);

        if (withJson)
            data.json = data.toJson();

        data.subStruct = TestDataWithInferredSchema.random(withJson);
        return data;
    }
}
