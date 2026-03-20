package org.sv.flexobject.arrow.testdata;

import com.carfax.dt.streaming.StreamableImpl;
import com.carfax.dt.streaming.schema.annotations.ValueClass;
import com.carfax.dt.streaming.testdata.levelone.leveltwo.SimpleObject;

import java.util.ArrayList;
import java.util.List;

public class SubSchemaInList extends StreamableImpl {
    public int intField;
    public String stringField;
    @ValueClass(valueClass = SimpleObject.class)
    public List<SimpleObject> listOfObjects = new ArrayList<>();

    public static SubSchemaInList random(){
        SubSchemaInList subSchemaInList = new SubSchemaInList();
        subSchemaInList.intField = (int) (Math.random() * 100);
        subSchemaInList.stringField = "stringValue" + (Math.random() * 100);
        subSchemaInList.listOfObjects.add(SimpleObject.random());
        subSchemaInList.listOfObjects.add(SimpleObject.random());
        subSchemaInList.listOfObjects.add(SimpleObject.random());
        return subSchemaInList;
    }
}
