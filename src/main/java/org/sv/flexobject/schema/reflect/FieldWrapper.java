package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.SchemaException;
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

    protected String fieldName;
    protected Class<?> clazz;

    public FieldWrapper(Class<?> clazz, String fieldName) {
        this.fieldName = fieldName;
        this.clazz = clazz;
    }

    public Field getField() throws NoSuchFieldException {
        if (field == null) {
            field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            fieldClass = field.getType();
            type = DataTypes.valueOf(fieldClass);

            if (type == DataTypes.invalid){
                ValueType vt = field.getAnnotation(ValueType.class);
                if (vt != null){
                    type = vt.type();
                    if (Map.class.isAssignableFrom(fieldClass))
                        structure = STRUCT.map;
                    else if (List.class.isAssignableFrom(fieldClass))
                        structure = STRUCT.list;
                }
            } else if (fieldClass.isArray())
                structure = STRUCT.array;
            else
                structure = STRUCT.scalar;
        }
        return field;
    }


    public Object getValue(Object o) throws NoSuchFieldException, IllegalAccessException, SchemaException {
        Object fieldValue = getField().get(o);
        if (fieldValue == null){
            if (getFieldClass().isArray())
                throw new SchemaException("Arrays must be initialized in data objects with Schema. Field " + fieldName + " in class " + clazz.getName());
            if (structure == STRUCT.list) {
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

    public void setValue(Object o, Object value) throws NoSuchFieldException, IllegalAccessException {
        getField().set(o,value);
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
                Arrays.fill((Object[]) getValue(o), null);
            }catch (ClassCastException e){
                throw new SchemaException("Only arrays of non-primitive types are allowed in data objects with Schema. Field " + fieldName + " in class " + clazz.getName(), e);
            }
        } else if (Collection.class.isAssignableFrom(getFieldClass())){
            ((Collection)getValue(o)).clear();
        }else if (ContainerNode.class.isAssignableFrom(getFieldClass())){
            ((ContainerNode)getValue(o)).removeAll();
        }else
            getField().set(o, null);
    }

    public DataTypes getType() throws NoSuchFieldException {
        if (field == null)
            getField();
        return type;
    }

    public STRUCT getStructure() throws NoSuchFieldException {
        if (field == null)
            getField();
        return structure;
    }

    public Class<?> getFieldClass() throws NoSuchFieldException {
        if (field == null)
            getField();
        return fieldClass;
    }
}
