package org.sv.flexobject.hadoop.streaming.avro;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.annotations.ValueClass;
import org.sv.flexobject.testdata.TestDataWithInferredSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FooBar extends StreamableWithSchema {

    public FooBar() throws NoSuchFieldException {
    }

//    public int intField;
//    public Integer intFieldOptional;
//    public ObjectNode json;
    @ValueClass(valueClass = Foo.class)
    public Foo[] subStructArray = new Foo[10];
    @ValueClass(valueClass = Foo.class)
    public List<Foo> subStructList = new ArrayList<>();
    @ValueClass(valueClass = Foo.class)
    public Map<String, Foo> subStructMap = new HashMap<>();

    public static FooBar random(boolean withJson) throws Exception {
        FooBar data = new FooBar();
//        data.intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
//        data.intFieldOptional = (int)Math.round(Math.random() * Integer.MAX_VALUE);
//
//        if (withJson)
//            data.json = data.toJson();

        for (int i = 0 ; i < 5 ; ++i )
            data.subStructArray[i] = Foo.random(true);
        for (int i = 0 ; i < 5 ; ++i )
            data.subStructList.add(Foo.random(true));
        for (int i = 0 ; i < 5 ; ++i )
            data.subStructMap.put("value" + i, Foo.random(true));
        return data;
    }
}
