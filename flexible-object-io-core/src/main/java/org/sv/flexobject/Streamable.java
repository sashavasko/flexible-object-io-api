package org.sv.flexobject;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.adapter.MapInAdapter;
import org.sv.flexobject.adapter.MapOutAdapter;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.AbstractFieldDescriptor;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaException;

import java.util.Map;
import java.util.function.Supplier;

public interface Streamable extends Savable, Loadable {

    default Schema getSchema(){
        return Schema.getRegisteredSchema(getClass());
    }

    default void clear() throws SchemaException {
        getSchema().clear(this);
    }

    default boolean isEmpty() throws SchemaException {
        return getSchema().isEmpty(this);
    }

    @Override
    default boolean load(InAdapter input) throws Exception {
        clear();
        return getSchema().load(this, input) != null;
    }

    @Override
    default boolean save(OutAdapter output) throws Exception {
        return getSchema().save(this, output);
    }

    default Object get(String fieldName) throws Exception {
        return getSchema().getDescriptor(fieldName).get(this);
    }

    default Object get(AbstractFieldDescriptor descriptor) throws Exception {
        return descriptor.get(this);
    }

    default Object get(Enum field) throws Exception {
//        if (getSchema().isInferred())
//            throw new SchemaException("Enum Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        return getSchema().getFieldDescriptor(field).get(this);
    }

    default void set(String fieldName, Object value) throws Exception {
        getSchema().getDescriptor(fieldName).set(this, value);
    }

    default void set(AbstractFieldDescriptor descriptor, Object value) throws Exception {
//        if (getSchema().isInferred())
//            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        descriptor.set(this, value);
    }

    default void set(Enum field, Object value) throws Exception {
//        if (getSchema().isInferred())
//            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        getSchema().getFieldDescriptor(field).set(this, value);
    }

    default void fromJsonBytes(byte[] bytes) throws Exception {
        fromJsonBytes(bytes, Constants.DEFAULT_CHARSET);
    }

    default void fromJsonBytes(byte[] bytes, final String charset) throws Exception {
        String containerJsonString = new String(bytes, charset);
        JsonNode container = MapperFactory.getObjectReader().readTree(containerJsonString);
        JsonInputAdapter.consume(container, this::load);
    }

    default byte[] toJsonBytes() throws Exception {
        return toJsonBytes(Constants.DEFAULT_CHARSET);
    }

    default byte[] toJsonBytes(final String charset) throws Exception {
        ObjectNode container = JsonOutputAdapter.produce(this);
        return MapperFactory.getObjectWriter().writeValueAsString(container).getBytes(charset);
    }

    default ObjectNode toJson() throws Exception {
        return JsonOutputAdapter.produce(this);
    }

    default Map toHashMap() throws Exception {
        return MapOutAdapter.produceHashMap(this);
    }

    default Map toMap(Supplier<Map> mapFactory) throws Exception {
        return MapOutAdapter.produce(mapFactory, this);
    }

    default Map toMap(Class<? extends Map> outputMapClass) throws Exception {
        return MapOutAdapter.produce(outputMapClass, this);
    }

    default <T extends Streamable> T fromMap(Map map) throws Exception {
        return MapInAdapter.builder().from(map).build().consume(this) ? (T)this : null;
    }

    default <T extends Streamable> T fromJson(JsonNode json) throws Exception {
        return JsonInputAdapter.forValue(json).consume(this) ? (T)this : null;
    }

    default <T extends Streamable> T fromYaml(String yaml) throws Exception {
        return fromJson(MapperFactory.getYamlObjectReader().readTree(yaml));
    }

    static boolean equals(Streamable o1, Object o2) {
        if (o1 == o2) return true;
        if (o2 == null || o1.getClass() != o2.getClass()) return false;
        Streamable other = (Streamable) o2;
        if (!o1.getSchema().equals(other.getSchema()))
            return false;

        return o1.getSchema().compareFields(o1, other);
    }

    static String toString(Streamable o) {
        try {
            return o.toJson().toString();
        } catch (Exception e) {
            return o.getClass().getName() + "@" + Integer.toHexString(o.hashCode()) + "(" + e.getMessage() + ")";
        }
    }
}
