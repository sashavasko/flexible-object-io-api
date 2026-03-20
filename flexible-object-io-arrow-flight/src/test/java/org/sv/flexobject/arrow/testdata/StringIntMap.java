package org.sv.flexobject.arrow.testdata;

import com.carfax.dt.streaming.StreamableImpl;
import com.carfax.dt.streaming.schema.DataTypes;
import com.carfax.dt.streaming.schema.annotations.KeyType;
import com.carfax.dt.streaming.schema.annotations.ValueType;

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
