package org.sv.flexobject.hadoop.streaming.parquet.testdata;


import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.testdata.TestDataWithInferredSchemaAndList;

public class StreamableWithNonRepeatedSubschema extends StreamableWithSchema {

    public TestDataWithInferredSchemaAndList subSchema;

    public static StreamableWithNonRepeatedSubschema random() throws Exception {
        StreamableWithNonRepeatedSubschema data = new StreamableWithNonRepeatedSubschema();
        data.subSchema = TestDataWithInferredSchemaAndList.random(false);
        return data;
    }
}
