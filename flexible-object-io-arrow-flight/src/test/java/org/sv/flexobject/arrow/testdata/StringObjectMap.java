package org.sv.flexobject.arrow.testdata;

import com.carfax.dt.streaming.StreamableImpl;
import com.carfax.dt.streaming.schema.DataTypes;
import com.carfax.dt.streaming.schema.annotations.KeyType;
import com.carfax.dt.streaming.schema.annotations.ValueClass;
import com.carfax.dt.streaming.testdata.levelone.leveltwo.SimpleObject;

import java.util.HashMap;
import java.util.Map;

public class StringObjectMap extends StreamableImpl {

    public String stringField;
    @KeyType(type = DataTypes.string)
    @ValueClass(valueClass = SimpleObject.class)
    public Map<String, SimpleObject> map = new HashMap<>();

    public static StringObjectMap random() {
        StringObjectMap data = new StringObjectMap();
        data.stringField = "FOO" + ((int) (Math.random()*1000));
        data.map.put("ene", SimpleObject.random());
        data.map.put("nana", null);
        data.map.put("bene", SimpleObject.random());
        data.map.put("mo", SimpleObject.random());
        return data;
    }

}
