package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.schema.annotations.*;
import org.sv.flexobject.schema.reflect.*;
import org.sv.flexobject.util.BiConsumerWithException;
import org.sv.flexobject.util.FunctionWithException;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class FieldDescriptor {

    protected String name;
    protected DataTypes type;
    protected FunctionWithException getter;
    protected BiConsumerWithException setter;
    protected int order;

    public FieldDescriptor(String name, DataTypes type, FunctionWithException getter, BiConsumerWithException setter, int order) {
        this.name = name;
        this.type = type;
        this.getter = getter;
        this.setter = setter;
        this.order = order;
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

        if (fieldClass.isArray())
            externalType = DataTypes.jsonNode;
        else if (List.class.isAssignableFrom(fieldClass)
                || Map.class.isAssignableFrom(fieldClass)) {
            externalType = DataTypes.jsonNode;
        } else {
            ScalarFieldTyped sft = field.getAnnotation(ScalarFieldTyped.class);
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

    public void load(Object o, InAdapter adapter) throws Exception {
        Object value = type.get(adapter, name);
        if (value != null)
            set(o, value);
    }

    public void save(Object o, OutAdapter adapter) throws Exception {
        type.set(adapter, name, getter.apply(o));
    }

    public Object get(Object o) throws Exception {
        return getter.apply(o);
    }

    // Setter always merges data
    public void set(Object o, Object value) throws Exception {
        setter.accept(o, value);
    }

    public void clear(Object o) throws Exception {
        if (setter instanceof FieldWrapper)
            ((FieldWrapper)setter).clear(o);
        else
            setter.accept(o, null);
    }

    public int getOrder() {
        return order;
    }

    public String getName() {
        return name;
    }

    public DataTypes getType() {
        return type;
    }
}
