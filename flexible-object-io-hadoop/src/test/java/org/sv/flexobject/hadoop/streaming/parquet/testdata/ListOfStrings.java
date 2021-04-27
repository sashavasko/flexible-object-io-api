package org.sv.flexobject.hadoop.streaming.parquet.testdata;


import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.List;

public class ListOfStrings  extends StreamableWithSchema {
    @ValueType(type = DataTypes.string)
    public List<String> binaryFieldRepeated = new ArrayList<>();
    @ValueType(type = DataTypes.string)
    public List<String> binaryFieldSimpleRepeated = new ArrayList<>();
}
