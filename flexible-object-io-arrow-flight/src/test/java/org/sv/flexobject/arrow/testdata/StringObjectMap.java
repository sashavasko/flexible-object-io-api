package org.sv.flexobject.arrow.testdata;

import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.KeyType;
import org.sv.flexobject.schema.annotations.ValueClass;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;

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
