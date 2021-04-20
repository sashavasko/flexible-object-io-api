package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.annotations.*;
import org.sv.flexobject.schema.reflect.*;
import org.sv.flexobject.util.BiConsumerWithException;
import org.sv.flexobject.util.FunctionWithException;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class FieldDescriptor extends AbstractFieldDescriptor{

    protected DataTypes type;
    protected FunctionWithException getter;
    protected BiConsumerWithException setter;

    public FieldDescriptor(String name, DataTypes type, FunctionWithException getter, BiConsumerWithException setter, int order) {
        super(name, order);
        this.type = type;
        this.getter = getter;
        this.setter = setter;
    }

    public FieldDescriptor(Class<?> clazz, String name, DataTypes type, int order) {
        this(name, type, new ScalarGetter(clazz, name), new ScalarSetter(clazz, name), order);
    }

    public FieldDescriptor(Class<?> clazz, String name, DataTypes type, FunctionWithException getter, int order) {
        this(name, type, getter, new ScalarSetter(clazz, name), order);
    }

    public static FieldDescriptor NOP = new FieldDescriptor("", DataTypes.invalid, (f)->null, (f,o)->{}, -1);

    public static class Builder{
        String name;
        String classFieldName;
        DataTypes type;
        Class<?> clazz;
        int order;
        FunctionWithException getter;
        BiConsumerWithException setter;
        Integer indexInArray;
        Object keyInMap;
        String jsonPath;
        Function<ValueNode, Object> jsonGetter;
        Function<Object, JsonNode> jsonNodeMaker;

        private Builder(){}

        public Builder withName(String name){
            this.name = name;
            return this;
        }

        public Builder withClassFieldName(String name){
            this.classFieldName = name;
            return this;
        }

        public Builder withType(DataTypes type){
            this.type = type;
            return this;
        }

        public Builder withClass(Class<?> clazz){
            this.clazz = clazz;
            return this;
        }

        public Builder withOrder(int order){
            this.order = order;
            return this;
        }

        public Builder withGetter(FunctionWithException getter){
            this.getter = getter;
            return this;
        }

        public Builder withSetter(BiConsumerWithException setter){
            this.setter = setter;
            return this;
        }

        public Builder asArrayElement(int indexInArray){
            this.indexInArray = indexInArray;
            return this;
        }

        public Builder asMapEntry(Object keyInMap){
            this.keyInMap = keyInMap;
            return this;
        }

        public Builder asJson(String jsonPath){
            this.jsonPath = jsonPath;
            return this;
        }

        public Builder withJsonGetter(Function<ValueNode, Object> jsonGetter){
            this.jsonGetter = jsonGetter;
            return this;
        }

        public Builder withJsonNodeMaker(Function<Object, JsonNode> jsonNodeMaker){
            this.jsonNodeMaker = jsonNodeMaker;
            return this;
        }

        public FieldDescriptor build(){
            if (classFieldName == null)
                classFieldName = name;

            if (getter == null){
                if (indexInArray != null)
                    getter = new ArrayGetter(clazz, classFieldName, indexInArray);
                else if (keyInMap != null)
                    getter = new MapGetter(clazz, classFieldName, keyInMap);
                else if (jsonPath != null) {
                    if (jsonGetter != null)
                        getter = new JsonGetter(clazz, classFieldName, jsonPath, jsonGetter);
                    else
                        getter = new JsonGetter(clazz, classFieldName, jsonPath, type);
                } else
                    getter = new ScalarGetter(clazz, classFieldName);
            }
            if (setter == null){
                if (indexInArray != null)
                    setter = new ArraySetter(clazz, classFieldName, indexInArray);
                else if (keyInMap != null)
                    setter = new MapSetter(clazz, classFieldName, keyInMap);
                else if (jsonPath != null) {
                    if (jsonNodeMaker != null)
                        setter = new JsonSetter(clazz, classFieldName, jsonPath, jsonNodeMaker);
                    else
                        setter = new JsonSetter(clazz, classFieldName, jsonPath, type);

                } else
                    setter = new ScalarSetter(clazz, classFieldName);
            }

            return new FieldDescriptor(name, type, getter, setter, order);
        }

    }

    static FieldDescriptor fromField(Class<?> dataClass, Field field, int order) {
        Class<?> fieldClass = field.getType();
        String name = field.getName();
        DataTypes externalType;

        ScalarFieldTyped sft = field.getAnnotation(ScalarFieldTyped.class);
        if (fieldClass.isArray() && sft == null)
            externalType = DataTypes.jsonNode;
        else if (List.class.isAssignableFrom(fieldClass)
                || Map.class.isAssignableFrom(fieldClass)) {
            externalType = DataTypes.jsonNode;
        } else {
            externalType = sft != null ? sft.type() :DataTypes.valueOf(fieldClass);
        }

        return new FieldDescriptor(dataClass, name, externalType, order);
    }

    static FieldDescriptor fromEnum(Class<?> dataClass, Enum <?> e) throws NoSuchFieldException, SchemaException {
        Field field = e.getClass().getField(e.name());

        {
            ScalarField sf = field.getAnnotation(ScalarField.class);
            if (sf != null) {
                Class<?> fieldClass = dataClass.getDeclaredField(e.name()).getType();
                DataTypes type = DataTypes.invalid;
                if (fieldClass.isArray())
                    type = DataTypes.jsonNode;
                else {
                    type = DataTypes.valueOf(fieldClass);
                    if (type == DataTypes.invalid
                            && (List.class.isAssignableFrom(fieldClass)
                            || Map.class.isAssignableFrom(fieldClass))) {
                        type = DataTypes.jsonNode;
                    }
                }
                return new FieldDescriptor(dataClass, e.name(), type, e.ordinal());
            }
        }
        {
            ScalarFieldTyped sft = field.getAnnotation(ScalarFieldTyped.class);
            if (sft != null)
                return new FieldDescriptor(dataClass, e.name(), sft.type(), e.ordinal());
        }
        {
            ArrayField af = field.getAnnotation(ArrayField.class);
            if (af != null) {
                DataTypes classFieldDataType = DataTypes.valueOf(dataClass.getDeclaredField(af.classFieldName()).getType());
                if (classFieldDataType == DataTypes.invalid){ // due to Type erasure in case array is a List
                    classFieldDataType = new FieldWrapper(dataClass, af.classFieldName()).getType();
                }
                return builder().withClass(dataClass).withName(e.name()).withType(classFieldDataType).
                        withOrder(e.ordinal()).withClassFieldName(af.classFieldName()).asArrayElement(af.index()).build();
            }
        }
        {
            ArrayFieldTyped aft = field.getAnnotation(ArrayFieldTyped.class);
            if (aft != null) {
                return builder().withClass(dataClass).withName(e.name()).withType(aft.type()).withOrder(e.ordinal()).withClassFieldName(aft.classFieldName()).asArrayElement(aft.index()).build();
            }
        }
        {
            MapField mf = field.getAnnotation(MapField.class);
            if (mf != null)
                return builder().withClass(dataClass).withName(e.name()).withType(mf.type()).withOrder(e.ordinal()).withClassFieldName(mf.classFieldName()).asMapEntry(mf.key()).build();
        }
        {
            MapWithIntKeyField mif = field.getAnnotation(MapWithIntKeyField.class);
            if (mif != null)
                return builder().withClass(dataClass).withName(e.name()).withType(mif.type()).withOrder(e.ordinal()).withClassFieldName(mif.classFieldName()).asMapEntry(mif.key()).build();
        }
        {
            MapWithLongKeyField mlf = field.getAnnotation(MapWithLongKeyField.class);
            if (mlf != null)
                return builder().withClass(dataClass).withName(e.name()).withType(mlf.type()).withOrder(e.ordinal()).withClassFieldName(mlf.classFieldName()).asMapEntry(mlf.key()).build();
        }
        {
            JsonField jf = field.getAnnotation(JsonField.class);
            if (jf != null)
                return builder().withClass(dataClass).withName(e.name()).withType(jf.type()).withOrder(e.ordinal()).withClassFieldName(jf.classFieldName()).asJson(jf.path()).build();
        }
        return NOP;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void load(Object o, InAdapter adapter) throws SchemaException {
        Object value = null;
        try {
            value = type.get(adapter, name);
        }catch(Exception e){
            if (e instanceof SchemaException)
                throw (SchemaException) e;
            throw new SchemaException("Error reading field " + getQualifiedName(o) + " from adapter " + adapter.getClass().getName(), e);
        }
        if (value != null) {
            set(o, value);
        }
    }

    @Override
    public void save(Object o, OutAdapter adapter) throws SchemaException {
        try {
            type.set(adapter, name, getter.apply(o));
        }catch(Exception e){
            if (e instanceof SchemaException)
                throw (SchemaException) e;
            throw new SchemaException("Error saving field " + getQualifiedName(o), e);
        }
    }

    @Override
    public Object get(Object o) throws SchemaException {
        try {
            return getter.apply(o);
        }catch(Exception e){
            if (e instanceof SchemaException)
                throw (SchemaException) e;
            throw new SchemaException("Error getting field " + getQualifiedName(o), e);
        }
    }


    // Setter always merges data
    @Override
    public void set(Object o, Object value) throws SchemaException {
        try {
            setter.accept(o, value);
        }catch(Exception e){
            if (e instanceof SchemaException)
                throw (SchemaException) e;
            throw new SchemaException("Error setting field " + getQualifiedName(o), e);
        }
    }

    @Override
    public void clear(Object o) throws SchemaException {
        try {
            if (setter instanceof FieldWrapper)
                ((FieldWrapper) setter).clear(o);
            else
                setter.accept(o, null);
        }catch(Exception e) {
            if (e instanceof SchemaException)
                throw (SchemaException) e;
            throw new SchemaException("Error clearing field " + getQualifiedName(o), e);
        }
    }

    @Override
    public boolean isEmpty(StreamableWithSchema o) {
        try {
            if (getter instanceof FieldWrapper) {
                return ((FieldWrapper) getter).isEmpty(o);
            }else {
                return getter.apply(o) == null;
            }
        } catch (Exception e) {
            return false;
        }
    }

    public DataTypes getType() {
        return type;
    }

    public DataTypes getValueType() throws NoSuchFieldException, SchemaException {
        if (isScalar())
            return type;
        else if (setter instanceof FieldWrapper){
            return((FieldWrapper) setter).getType();
        }
        return null;

    }

    public Class<? extends StreamableWithSchema> getSubschema() throws NoSuchFieldException, SchemaException {
        if (type == DataTypes.jsonNode && setter instanceof FieldWrapper){
            return((FieldWrapper) setter).getValueClass();
        }
        return null;
    }

    public FieldWrapper.STRUCT getStructure() {
        try {
            if (setter instanceof FieldWrapper)
                return ((FieldWrapper) setter).getStructure();
        }catch (Exception e){}

        return FieldWrapper.STRUCT.unknown;
    }

    public boolean isScalar(){
        return type != DataTypes.jsonNode
                || getStructure() == FieldWrapper.STRUCT.scalar;
    }
}
