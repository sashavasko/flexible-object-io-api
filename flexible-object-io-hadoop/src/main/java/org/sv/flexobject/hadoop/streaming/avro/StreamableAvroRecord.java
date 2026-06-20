package org.sv.flexobject.hadoop.streaming.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.util.InstanceFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamableAvroRecord implements GenericRecord {

    Streamable wrappedObject;
    Schema schema;

    public StreamableAvroRecord() {
    }

    public StreamableAvroRecord(Streamable wrappedObject, Schema avroSchema) {
        set(wrappedObject, avroSchema);
    }

    public static StreamableAvroRecord forClass(Class<? extends Streamable> dataClass){
        StreamableAvroRecord record = new StreamableAvroRecord();
        record.schema = AvroSchema.forClass(dataClass);
        record.wrappedObject = InstanceFactory.get(dataClass);
        return record;
    }

    public void set(Streamable wrappedObject, Schema avroSchema) {
        this.wrappedObject = wrappedObject;
        schema = avroSchema;
    }

    @Override
    public void put(String key, Object v) {
        try {
            wrappedObject.set(key, v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(String key) {
        try {
            return wrappedObject.get(key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(int i, Object v) {
        try {
            org.sv.flexobject.schema.Schema wrappedSchema = wrappedObject.getSchema();
            FieldDescriptor descriptor = wrappedSchema.getDescriptor(i);
            Class<? extends Streamable> subSchema = descriptor.getSubschema();
            if (v instanceof Utf8) {
                descriptor.set(wrappedObject, v.toString());
            } else if (subSchema != null){
                if (v instanceof GenericRecord) {
                    Streamable subRecord = AvroSchema.convertGenericRecord((GenericRecord)v, subSchema);
                    descriptor.set(wrappedObject, subRecord);
                }else if (v instanceof List<?>) {
                    @SuppressWarnings("unchecked")
                    List<GenericRecord> listOfRecords = (List<GenericRecord>) v;
                    List<Streamable> usableList = new ArrayList<>(listOfRecords.size());
                    for (GenericRecord genericRecord : listOfRecords) {
                        if (genericRecord == null)
                            usableList.add(null);
                        else
                            usableList.add(AvroSchema.convertGenericRecord(genericRecord, subSchema));
                    }
                    descriptor.set(wrappedObject, usableList);
                }else if (v instanceof Map<?,?>) {
                    @SuppressWarnings("unchecked")
                    Map<?,GenericRecord> mapOfRecords = (Map<?,GenericRecord>) v;
                    Map<Object, Streamable> usableMap = new HashMap<>();
                    for (Map.Entry<?,GenericRecord> entry : mapOfRecords.entrySet()) {
                        Object key = entry.getKey();
                        if (entry.getValue() == null)
                            usableMap.put(key, null);
                        else
                            usableMap.put(key, AvroSchema.convertGenericRecord(entry.getValue(), subSchema));
                    }
                    descriptor.set(wrappedObject, usableMap);
                }else
                    throw new SchemaException("Unknown Avro collection class: " + v.getClass().getName());
            }else {
                descriptor.set(wrappedObject, v);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(int i) {
        try {
            org.sv.flexobject.schema.Schema internalSchema = wrappedObject.getSchema();
            if (internalSchema == null)
                throw new SchemaException("Missing schema for class " + wrappedObject.getClass());
            FieldDescriptor descriptor = internalSchema.getDescriptor(i);
            Object value = descriptor.get(wrappedObject);

            return AvroSchema.toAvro(descriptor, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return "StreamableAvroRecord{" +
                "wrappedObject=" + wrappedObject +
                ", schema=" + schema +
                '}';
    }
}
