package org.sv.flexobject.arrow.testdata;

import com.carfax.dt.streaming.StreamableImpl;
import com.carfax.dt.streaming.schema.DataTypes;
import com.carfax.dt.streaming.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.List;

public class IntList extends StreamableImpl {

    public String stringField;
    @ValueType(type = DataTypes.int32)
    public List<Integer> intList = new ArrayList<>();

    public static IntList random() {
        IntList intList = new IntList();
        intList.stringField = "FOO" + ((int) (Math.random()*1000));
        intList.intList.add((int) (Math.random()*1000));
        intList.intList.add(null);
        intList.intList.add((int) (Math.random()*1000));
        intList.intList.add((int) (Math.random()*1000));
        return intList;
    }

}
