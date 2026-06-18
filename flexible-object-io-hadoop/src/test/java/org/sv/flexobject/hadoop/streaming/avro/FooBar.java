package org.sv.flexobject.hadoop.streaming.avro;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.testdata.TestDataWithInferredSchema;

public class FooBar extends StreamableWithSchema {

    public FooBar() throws NoSuchFieldException {
    }

    public int intField;
    public Integer intFieldOptional;
    public ObjectNode json;
    public Foo subStruct;

    public static FooBar random(boolean withJson) throws Exception {
        FooBar data = new FooBar();
        data.intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        data.intFieldOptional = (int)Math.round(Math.random() * Integer.MAX_VALUE);

        if (withJson)
            data.json = data.toJson();

        data.subStruct = Foo.random(withJson);
        return data;
    }
}
