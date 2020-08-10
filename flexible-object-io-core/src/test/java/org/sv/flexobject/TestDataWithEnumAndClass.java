package org.sv.flexobject;

import org.sv.flexobject.schema.annotations.EnumSetField;

import java.util.EnumSet;
import java.util.Set;

public class TestDataWithEnumAndClass extends StreamableWithSchema {

    public enum TestEnum {
        uno,
        dos,
        tres
    }

    public TestDataWithEnumAndClass() throws NoSuchFieldException {
    }

    public Class clazz;
    public TestEnum enumValue;
    @EnumSetField(enumClass = TestEnum.class, emptyValue = "none")
    public Set<TestEnum> enumSet = EnumSet.noneOf(TestEnum.class);
}
