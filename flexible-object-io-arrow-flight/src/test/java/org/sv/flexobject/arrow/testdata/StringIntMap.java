package org.sv.flexobject.arrow.testdata;

import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.KeyType;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.HashMap;
import java.util.Map;

public class StringIntMap extends StreamableImpl {

    public String stringField;
    @KeyType(type = DataTypes.string)
    @ValueType(type = DataTypes.int32)
    public Map<String, Integer> map = new HashMap<>();

    public static StringIntMap random() {
        StringIntMap data = new StringIntMap();
        data.stringField = "FOO" + ((int) (Math.random()*1000));
        data.map.put("ene", (int) (Math.random()*1000));
        data.map.put("nana", null);
        data.map.put("bene", (int) (Math.random()*1000));
        data.map.put("mo", (int) (Math.random()*1000));
        return data;
    }

}
