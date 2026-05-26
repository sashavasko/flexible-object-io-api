package org.sv.flexobject.schema.describe;

import io.github.classgraph.ClassInfo;
import org.sv.flexobject.schema.DataTypes;

public class ValueDescriptor extends BasicDescriptor<ValueDescriptor> {
    String value;


    public static ValueDescriptor forClassInfo(ClassInfo classInfo) {
        return forClass(classInfo.getName());
    }

    public static ValueDescriptor forClass(String className) {
        ValueDescriptor descriptor = new ValueDescriptor();
        descriptor.value = className;
        try {
            Class clazz = DataTypes.classConverter(className);
            descriptor.from((PropertyDescription) clazz.getAnnotation(PropertyDescription.class));
        } catch (Exception e) {
        }
        return descriptor;
    }

    public static ValueDescriptor forEnumConstant(Enum enumConstant) {
        ValueDescriptor descriptor = new ValueDescriptor();
        descriptor.value = enumConstant.name();
        try {
            descriptor.from(enumConstant.getClass().getField(enumConstant.name()).getAnnotation(PropertyDescription.class));
        } catch (Exception e) {
        }
        return descriptor;
    }

    public String getValue() {
        return value;
    }
}
