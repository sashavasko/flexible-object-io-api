package org.sv.flexobject.schema.describe;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import org.sv.flexobject.properties.PropertiesWrapper;
import org.sv.flexobject.schema.*;
import org.sv.flexobject.schema.annotations.EnumSetField;
import org.sv.flexobject.schema.annotations.ValueClass;
import org.sv.flexobject.schema.reflect.FieldWrapper;
import org.sv.flexobject.translate.SeparatorTranslator;
import org.sv.flexobject.util.InstanceFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class PropertyDescriptor extends BasicDescriptor<PropertyDescriptor> {

    String propertyName;
    DataTypes dataType;
    FieldWrapper.STRUCT structure;

    @ValueClass(valueClass = ValueDescriptor.class)
    List<ValueDescriptor> availableValues = new ArrayList<>();

    PropertiesDescriptor subProperties;

    public PropertyDescriptor() {
    }

    public static Stream<ValueDescriptor> describeEnumConstants(Class<? extends Enum> enumClass){
        return Arrays.stream(enumClass.getEnumConstants()).map(ValueDescriptor::forEnumConstant);
    }

    public PropertyDescriptor(Class<?> ownerClass, SchemaElement field) {
        if (PropertiesWrapper.class.isAssignableFrom(ownerClass)) {
            PropertiesWrapper propertiesWrapper = (PropertiesWrapper) InstanceFactory.get(ownerClass);
            this.propertyName = propertiesWrapper.getSettingName(field.getName());
        } else {
            this.propertyName = field.getName();
        }
        this.displayName = SeparatorTranslator.translate(this.propertyName, " ");
        AbstractFieldDescriptor fieldDescriptor = field.getDescriptor();
        dataType = fieldDescriptor.getType();
        structure = fieldDescriptor.getStructure();

        Field reflectionField = null;
        Class<?> subSchema = null;
        try {
            subSchema = fieldDescriptor.getSubschema();
            reflectionField = ownerClass.getField(field.getName());
        } catch (NoSuchFieldException e) {
        }

        if (subSchema != null) {
            subProperties = Schema.describe(subSchema);
        }

        if (((FieldDescriptor) fieldDescriptor).getSetter() instanceof FieldWrapper) {
            FieldWrapper fieldWrapper = (FieldWrapper) ((FieldDescriptor) fieldDescriptor).getSetter();
            try {
                reflectionField = fieldWrapper.getField();
            } catch (NoSuchFieldException e) {
            }
            if (fieldWrapper.isEnum()) {
                Class<? extends Enum> enumClass = fieldWrapper.getEnumClass();
                if (enumClass == null) {
                    try {
                        enumClass = (Class<? extends Enum>) fieldWrapper.getFieldClass();
                    } catch (NoSuchFieldException e) {
                    }
                }
                describeEnumConstants(enumClass).forEach(availableValues::add);
            } else {
                try {
                    if (Set.class.isAssignableFrom(fieldWrapper.getFieldClass()) && reflectionField != null) {
                        EnumSetField enumSetField = reflectionField.getAnnotation(EnumSetField.class);
                        if (enumSetField != null) {
                            describeEnumConstants(enumSetField.enumClass()).forEach(availableValues::add);
                        }
                        structure = FieldWrapper.STRUCT.list;
                    }
                } catch (NoSuchFieldException e) {
                }
            }
        }

        if (reflectionField != null) {
            from(reflectionField.getAnnotation(PropertyDescription.class));

            AvailableClassValues classValuesAnnotation = reflectionField.getAnnotation(AvailableClassValues.class);
            if (classValuesAnnotation != null) {
                try (ScanResult scanResult = new ClassGraph().whitelistPackages(classValuesAnnotation.namespace())
                        .enableClassInfo().scan()) {
                    ClassInfoList classList = null;
                    if (classValuesAnnotation.implement() != Void.class) {
                        classList = scanResult.getClassesImplementing(classValuesAnnotation.implement());
                    } else if (classValuesAnnotation.extend() != Void.class) {
                        classList = scanResult.getSubclasses(classValuesAnnotation.extend());
                    }
                    if (classList != null) {
                        classList.stream()
                                .map(ValueDescriptor::forClassInfo)
                                .forEach(availableValues::add);
                    }
                }
            }
        }
    }

    public String getPropertyName() {
        return propertyName;
    }

    public DataTypes getDataType() {
        return dataType;
    }

    public FieldWrapper.STRUCT getStructure() {
        return structure;
    }

    public List<ValueDescriptor> getAvailableValues() {
        return availableValues;
    }

    public PropertiesDescriptor getSubProperties() {
        return subProperties;
    }
}
