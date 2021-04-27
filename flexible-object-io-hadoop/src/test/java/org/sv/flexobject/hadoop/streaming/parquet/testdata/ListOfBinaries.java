package org.sv.flexobject.hadoop.streaming.parquet.testdata;


import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.List;

public class ListOfBinaries extends StreamableWithSchema {
    @ValueType(type = DataTypes.binary)
    public List<String> binaryFieldRepeated = new ArrayList<>();

}
