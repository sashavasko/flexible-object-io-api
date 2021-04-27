package org.sv.flexobject.hadoop.streaming.parquet.testdata;


import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.annotations.ValueClass;
import org.sv.flexobject.testdata.TestDataWithInferredSchemaAndList;

import java.util.HashMap;
import java.util.Map;

public class StreamableWithMapOfObjects extends StreamableWithSchema {

    @ValueClass(valueClass = TestDataWithInferredSchemaAndList.class)
    public Map<String, TestDataWithInferredSchemaAndList> mapOfObjects = new HashMap<>();

    public static StreamableWithMapOfObjects random() throws Exception {
        StreamableWithMapOfObjects data = new StreamableWithMapOfObjects();
        for (int i = 0  ; i < 5 ; i++){
            data.mapOfObjects.put("item" + i, TestDataWithInferredSchemaAndList.random(false));
        }

        return data;
    }
}
