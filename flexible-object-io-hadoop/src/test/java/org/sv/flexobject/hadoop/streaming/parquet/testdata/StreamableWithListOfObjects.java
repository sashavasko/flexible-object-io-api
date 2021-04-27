package org.sv.flexobject.hadoop.streaming.parquet.testdata;


import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.annotations.ValueClass;
import org.sv.flexobject.testdata.TestDataWithInferredSchemaAndList;

import java.util.ArrayList;
import java.util.List;

public class StreamableWithListOfObjects extends StreamableWithSchema {

    @ValueClass(valueClass = TestDataWithInferredSchemaAndList.class)
    public List<TestDataWithInferredSchemaAndList> listOfObjects = new ArrayList<>();

    public static StreamableWithListOfObjects random() throws Exception {
        StreamableWithListOfObjects data = new StreamableWithListOfObjects();
        for (int i = 0  ; i < 5 ; i++){
            data.listOfObjects.add(TestDataWithInferredSchemaAndList.random(false));
        }

        return data;
    }
}
