package org.sv.flexobject.testdata;

import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.annotations.EnumSetField;

import java.util.EnumSet;
import java.util.Set;

public class TestDataWithEnumAndClass extends StreamableWithSchema {

    public enum TestEnum {
        uno,
        dos,
        tres
    }

    public TestDataWithEnumAndClass() {
    }

    public Class clazz;
    public TestEnum enumValue;
    @EnumSetField(enumClass = TestEnum.class, emptyValue = "none")
    public EnumSet<TestEnum> enumSet = EnumSet.noneOf(TestEnum.class);

    public static TestDataWithEnumAndClass random(){
        TestDataWithEnumAndClass d = new TestDataWithEnumAndClass();

        d.clazz = SimpleTestDataWithSchema.class;
        d.enumValue = TestEnum.uno;
        d.enumSet = EnumSet.of(TestEnum.tres, TestEnum.dos);

        return d;
    }

}
