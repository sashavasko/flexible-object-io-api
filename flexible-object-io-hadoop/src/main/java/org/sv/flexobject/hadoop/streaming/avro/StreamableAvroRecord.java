package org.sv.flexobject.hadoop.streaming.avro;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.schema.reflect.FieldWrapper;

import java.util.*;

public class StreamableAvroRecord implements GenericRecord {

    StreamableWithSchema wrappedObject;
    Schema schema;

    public StreamableAvroRecord() {
    }

    public StreamableAvroRecord(StreamableWithSchema wrappedObject, Schema avroSchema) {
        set(wrappedObject, avroSchema);
    }

    public void set(StreamableWithSchema wrappedObject, Schema avroSchema) {
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
            if (v instanceof Utf8)
                descriptor.set(wrappedObject, v.toString());
            else
                descriptor.set(wrappedObject, v);
        } catch (SchemaException e) {
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

            if (value == null)
                return null;

            DataTypes type = descriptor.getType();
            FieldWrapper.STRUCT structure = descriptor.getStructure();

            Class<? extends StreamableWithSchema> subSchema = descriptor.getSubschema();
            if (descriptor.getSubschema() != null){
                Schema avroSubSchema = AvroSchema.forClass(subSchema);
                switch(structure){
                    case array:
                        StreamableWithSchema[] array = (StreamableWithSchema[]) value;
                        List avroArray = new ArrayList(array.length);
                        for (StreamableWithSchema item : array) {
                            avroArray.add(item == null ? null : new StreamableAvroRecord(item, avroSubSchema));
                        }
                        return avroArray;
                    case list:
                        List<StreamableWithSchema> list = (List<StreamableWithSchema>) value;
                        List avroList = new ArrayList(list.size());
                        for (StreamableWithSchema item : list) {
                            avroList.add(item == null ? null : new StreamableAvroRecord(item, avroSubSchema));
                        }
                        return avroList;
                    case map :
                        Map<String, StreamableWithSchema> map = (Map<String, StreamableWithSchema>) value;
                        Map<Utf8, StreamableAvroRecord> avroMap = new HashMap<>();
                        for (Map.Entry<String, StreamableWithSchema> entry : map.entrySet()) {
                            StreamableWithSchema item = entry.getValue();
                            avroMap.put(new Utf8(entry.getKey()), item == null ? null : new StreamableAvroRecord(item, avroSubSchema));
                        }
                        return avroMap;
                    default:
                        return new StreamableAvroRecord((StreamableWithSchema) value, avroSubSchema);
                }
            }

            if (type == DataTypes.jsonNode){
                if (structure == FieldWrapper.STRUCT.array)
                    return Arrays.asList((Object[])value);
                // TODO add conversion of values
                return value instanceof JsonNode ? value.toString() : value;
            }

            Object converted = descriptor.getType().convert(value);
            return converted;
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
