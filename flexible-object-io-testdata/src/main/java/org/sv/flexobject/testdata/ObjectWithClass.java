package org.sv.flexobject.testdata;

import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObjectWithClass extends StreamableWithSchema {

    public Class classField;

    public Class[] classArray = new Class[5];
    @ValueType(type= DataTypes.classObject)
    public List<Class> classList = new ArrayList<>();
    @ValueType(type= DataTypes.classObject)
    public Map<String, Class> classMap = new HashMap<>();

    public static ObjectWithClass random(){
        ObjectWithClass value = new ObjectWithClass();

        value.classField = ObjectWithDate.class;
        value.classArray[1] = ObjectWithClass.class;
        value.classList.add(ObjectWithClass.class);
        value.classMap.put("foo", ObjectWithClass.class);

        return value;
    }
}
