package org.sv.flexobject.mongo.schema.testdata;


import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.EnumSetField;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

public class ObjectWithSetOfLongs extends StreamableImpl {
    public enum TestEnum {
        one,
        two,
        three
    }

    @ValueType(type = DataTypes.int64)
    public Set<Long> set = new HashSet<>();

    @EnumSetField(enumClass = TestEnum.class, emptyValue = "none")
    public EnumSet<TestEnum> enumSet = EnumSet.noneOf(TestEnum.class);

    public void add(long val){
        set.add(val);
    }

    public void add(TestEnum val){
        enumSet.add(val);
    }
}
