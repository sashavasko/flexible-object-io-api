package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.schema.annotations.EnumSetField;
import org.sv.flexobject.schema.annotations.KeyType;
import org.sv.flexobject.schema.annotations.ValueClass;
import org.sv.flexobject.schema.annotations.ValueType;

import java.lang.reflect.Field;
import java.util.*;

public class FieldWrapper {

    public enum STRUCT{
        scalar,
        array,
        list,
        map,
        unknown
    }

    private Field field;
    private DataTypes type;
    private STRUCT structure = STRUCT.unknown;
    private Class<?> fieldClass;
    private boolean isEnum;
    private Class<? extends Enum> enumClass;
    private String emptyValue;

    protected String fieldName;
    protected Class<?> clazz;
    protected Class<? extends StreamableWithSchema> valueClass; // For Collections
    protected DataTypes keyType = DataTypes.string; // for Maps

    public FieldWrapper(Class<?> clazz, String fieldName) {
        this.fieldName = fieldName;
        this.clazz = clazz;
    }

    private STRUCT figureOutCollectionStructure() throws SchemaException {
        if (Map.class.isAssignableFrom(fieldClass))
            return STRUCT.map;
        else if (List.class.isAssignableFrom(fieldClass) || Set.class.isAssignableFrom(fieldClass))
            return STRUCT.list;
        else if (fieldClass.isArray())
            return STRUCT.array;
        else if (enumClass == null)
            throw new SchemaException(getQualifiedName() + ":  has unsupported collection class:" + fieldClass + " Only Lists and Maps and EnumSets are supported.");
        return structure;
    }

    public Field getField() throws NoSuchFieldException, SchemaException {
        if (field == null) {
            field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            fieldClass = field.getType();
            type = DataTypes.valueOf(fieldClass);
            isEnum = Enum.class.isAssignableFrom(fieldClass);
            if (Set.class.isAssignableFrom(fieldClass)){
                EnumSetField enumSetField = field.getAnnotation(EnumSetField.class);
                if (enumSetField != null){
                    enumClass = enumSetField.enumClass();
                    emptyValue = enumSetField.emptyValue();
                } else {
                    ValueType vt = field.getAnnotation(ValueType.class);
                    if (vt != null) {
                        type = vt.type();
                        structure = figureOutCollectionStructure();
                        return field;
                    }else
                        throw new SchemaException(getQualifiedName() + ": Sets are supported only for Enumeration type (with EnumSetField annotation) or ValueType annotation must be used.");
                }
            }

            if (type == DataTypes.invalid){
                ValueType vt = field.getAnnotation(ValueType.class);
                ValueClass vc = field.getAnnotation(ValueClass.class);
                if (vt != null){
                    type = vt.type();
                    structure = figureOutCollectionStructure();
                    if (structure == STRUCT.map){
                        KeyType kt = field.getAnnotation(KeyType.class);
                        if (kt != null)
                            keyType = kt.type();
                    }
                }else if (vc != null) {
                    type = DataTypes.jsonNode;
                    valueClass = vc.valueClass();
                    structure = figureOutCollectionStructure();
                }else if (fieldClass.isArray() && StreamableWithSchema[].class.isAssignableFrom(fieldClass)){
                    type = DataTypes.jsonNode;
                    structure = STRUCT.array;
                }
            } else if (fieldClass.isArray()) {
                structure = STRUCT.array;
            } else {
                structure = STRUCT.scalar;
                if (type == DataTypes.jsonNode && StreamableWithSchema.class.isAssignableFrom(fieldClass))
                    valueClass = (Class<? extends StreamableWithSchema>) fieldClass;
            }
        }
        return field;
    }


    public Object getValue(Object o) throws Exception {
        Object fieldValue = getField().get(o);
        if (fieldValue == null){
            if (getFieldClass().isArray()) {
                if (getType() == DataTypes.binary){
                    return null;
                }else
                    throw new SchemaException(getQualifiedName() + ": Arrays must be initialized in data objects with Schema.");
            }else if (structure == STRUCT.list) {
                fieldValue = new ArrayList<>();
                setValue(o, fieldValue);
            } else if (structure == STRUCT.map) {
                fieldValue = new HashMap<>();
                setValue(o, fieldValue);
            }else if (ArrayNode.class.isAssignableFrom(getFieldClass())){
                fieldValue = JsonNodeFactory.instance.arrayNode();
                setValue(o, fieldValue);
            }else if (ObjectNode.class.isAssignableFrom(getFieldClass())){
                fieldValue = JsonNodeFactory.instance.objectNode();
                setValue(o, fieldValue);
            }
        }
        return fieldValue;
    }

    public void setValue(Object o, Object value) throws Exception {
        if (field == null)
            getField();
        if (value == null) {
            field.set(o, null);
        } else if (isEnum){
            field.set(o, DataTypes.enumConverter(value, (Class<? extends Enum>) fieldClass));
        }else if (enumClass != null) {
            field.set(o, DataTypes.enumSetConverter(value, enumClass, emptyValue));
        }else
            field.set(o, value);
    }

    public void clear(Object o) throws Exception {
        if (field == null)
            getField();
        if (fieldClass.isPrimitive()){
            if (fieldClass == boolean.class)
                setValue(o, false);
            else
                setValue(o, 0);
        } else if (fieldClass.isArray()){
            try {
                Object[] array = (Object[]) getValue(o);
                if (valueClass != null){
                    for (Object elem : array){
                        if (elem != null)
                            ((StreamableWithSchema)elem).clear();
                    }
                }else
                    Arrays.fill(array, null);
            }catch (ClassCastException e){
                throw new SchemaException(getQualifiedName() + ": Only arrays of non-primitive types are allowed in data objects with Schema.", e);
            }
        } else if (Collection.class.isAssignableFrom(getFieldClass())){
            ((Collection)getValue(o)).clear();
        } else if (Map.class.isAssignableFrom(getFieldClass())){
            ((Map)getValue(o)).clear();
        }else if (ContainerNode.class.isAssignableFrom(getFieldClass())){
            ((ContainerNode)getValue(o)).removeAll();
        }else if (valueClass != null){
            Object value = getField().get(o);
            if (value != null)
                ((StreamableWithSchema)value).clear();
        }else
            getField().set(o, null);
    }

    public boolean isEmpty(StreamableWithSchema o) throws Exception {
        if (field == null)
            getField();

        if (fieldClass.isPrimitive()){
            Object value = getValue(o);
            return DataTypes.isEmptyPrimitive(value);
        } else if (fieldClass.isArray()){
            try {
                Object[] array = (Object[]) getValue(o);
                if (valueClass != null){
                    for (Object elem : array){
                        if (elem != null && !((StreamableWithSchema)elem).isEmpty())
                            return false;
                    }
                }else {
                    for (Object elem : array)
                        if (elem != null)
                            return false;
                }
            }catch (ClassCastException e){
                throw new SchemaException(getQualifiedName() + ": Only arrays of non-primitive types are allowed in data objects with Schema.", e);
            }
            return true;
        } else if (Collection.class.isAssignableFrom(getFieldClass())){
            return ((Collection)getValue(o)).isEmpty();
        } else if (Map.class.isAssignableFrom(getFieldClass())){
            return ((Map)getValue(o)).isEmpty();
        }else if (ContainerNode.class.isAssignableFrom(getFieldClass())){
            return !(((ContainerNode)getValue(o)).fields().hasNext());
        }else if (valueClass != null){
            Object value = getField().get(o);
            return (value == null || ((StreamableWithSchema)value).isEmpty());
        }

        return getField().get(o) == null;
    }

    public DataTypes getType() throws NoSuchFieldException, SchemaException {
        if (field == null)
            getField();
        return type;
    }

    public STRUCT getStructure() throws NoSuchFieldException, SchemaException {
        if (field == null)
            getField();
        return structure;
    }

    public Class<?> getFieldClass() throws NoSuchFieldException, SchemaException {
        if (field == null)
            getField();
        return fieldClass;
    }

    public Class<? extends StreamableWithSchema> getValueClass() throws NoSuchFieldException, SchemaException {
        if (field == null)
            getField();
        return valueClass;
    }

    protected Object enumSetAsString(Object value) throws Exception {
        if (value != null && value instanceof Set && enumClass != null){
            return DataTypes.enumSetToString(value, enumClass, emptyValue);
        }
        return value;
    }

    public String getQualifiedName(){
        return clazz.getName() + "." + fieldName;
    }
}
