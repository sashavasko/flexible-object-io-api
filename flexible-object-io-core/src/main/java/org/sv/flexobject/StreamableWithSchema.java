package org.sv.flexobject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.adapter.MapInAdapter;
import org.sv.flexobject.adapter.MapOutAdapter;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaException;

import java.util.Map;
import java.util.function.Supplier;

public class StreamableWithSchema<T extends SchemaElement> implements Streamable {

    public static final String DEFAULT_CHARSET = "UTF-8";
    Schema schema;

    public StreamableWithSchema() {
        schema = Schema.getRegisteredSchema(getClass());
    }

    public StreamableWithSchema(T[] fields) throws NoSuchFieldException, SchemaException {
        if (!Schema.isRegisteredSchema(getClass())){
            schema = new Schema(getClass(), fields);
        }
    }

    public StreamableWithSchema(Enum<?>[] fields) throws NoSuchFieldException, SchemaException {
        if (!Schema.isRegisteredSchema(getClass())){
            schema = new Schema(getClass(), fields);
        }
    }


    public Schema getSchema(){
        if (schema == null)
            schema = Schema.getRegisteredSchema(getClass());
        return schema;
    }

    public void clear() throws SchemaException {
        getSchema().clear(this);
    }

    public boolean isEmpty() throws SchemaException {
        return getSchema().isEmpty(this);
    }

    @Override
    public boolean load(InAdapter input) throws Exception {
        clear();
        return getSchema().load(this, input) != null;
    }

    @Override
    public boolean save(OutAdapter output) throws Exception {
        return getSchema().save(this, output);
    }

    public Object get(String fieldName) throws Exception {
        return getSchema().getDescriptor(fieldName).get(this);
    }

    public Object get(T field) throws Exception {
        if (getSchema().isInferred())
            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        return field.getDescriptor().get(this);
    }

    public Object get(Enum field) throws Exception {
        if (getSchema().isInferred())
            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        return getSchema().getFieldDescriptor(field).get(this);
    }

    public void set(String fieldName, Object value) throws Exception {
        getSchema().getDescriptor(fieldName).set(this, value);
    }

    public void set(T field, Object value) throws Exception {
        if (getSchema().isInferred())
            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        field.getDescriptor().set(this, value);
    }

    public void set(Enum field, Object value) throws Exception {
        if (getSchema().isInferred())
            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        getSchema().getFieldDescriptor(field).set(this, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamableWithSchema other = (StreamableWithSchema) o;
        if (!getSchema().equals(other.getSchema()))
            return false;

        return getSchema().compareFields(this, other);
    }

    public void fromJsonBytes(byte[] bytes) throws Exception {
        fromJsonBytes(bytes, DEFAULT_CHARSET);
    }

    public void fromJsonBytes(byte[] bytes, final String charset) throws Exception {
        String containerJsonString = new String(bytes, charset);
        JsonNode container = MapperFactory.getObjectReader().readTree(containerJsonString);
        JsonInputAdapter.consume(container, this::load);
    }

    public byte[] toJsonBytes() throws Exception {
        return toJsonBytes(DEFAULT_CHARSET);
    }

    public byte[] toJsonBytes(final String charset) throws Exception {
        ObjectNode container = JsonOutputAdapter.produce(this);
        return MapperFactory.getObjectWriter().writeValueAsString(container).getBytes(charset);
    }

    public ObjectNode toJson() throws Exception {
        return JsonOutputAdapter.produce(this);
    }

    public Map toHashMap() throws Exception {
        return MapOutAdapter.produceHashMap(this);
    }

    public Map toMap(Supplier<Map> mapFactory) throws Exception {
        return MapOutAdapter.produce(mapFactory, this);
    }

    public Map toMap(Class<? extends Map> outputMapClass) throws Exception {
        return MapOutAdapter.produce(outputMapClass, this);
    }

    public StreamableWithSchema fromMap(Map map) throws Exception {
        return MapInAdapter.forValue(map).consume(this) ? this : null;
    }

    public StreamableWithSchema fromJson(JsonNode json) throws Exception {
        return JsonInputAdapter.forValue(json).consume(this) ? this : null;
    }

    @Override
    public String toString() {
        try {
            return toJson().toString();
        } catch (Exception e) {
            return super.toString();
        }
    }
}
