package org.sv.flexobject.testdata;

import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.KeyType;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.HashMap;
import java.util.Map;

public class MapWithTypedKey extends StreamableWithSchema {

    @KeyType(type= DataTypes.int64)
    @ValueType(type=DataTypes.int64)
    public Map<Long,Long> mapWithLongKey = new HashMap<>();
}
