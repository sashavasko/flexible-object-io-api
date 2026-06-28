package org.sv.flexobject.avro.read;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.avro.StreamableAvroRecord;

public class StreamableGenericData extends GenericData {

    private static final GenericData INSTANCE = new StreamableGenericData();

    public static GenericData get(){
        return INSTANCE;
    }

    private StreamableGenericData(){}

    @Override
    public Object newRecord(Object old, Schema schema) {
        if (old instanceof IndexedRecord) {
            IndexedRecord record = (IndexedRecord) old;
            if (record.getSchema() == schema)
                return record;
        }

        try {
            @SuppressWarnings("unchecked")
            Class<? extends Streamable> loadedClass = (Class<? extends Streamable>) getClassLoader().loadClass(schema.getFullName());
            return StreamableAvroRecord.forClass(loadedClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object createString(Object value) {
        // Strings are immutable
        if (value instanceof String) {
            return value;
        }

        // Some CharSequence subclasses are mutable, so we still need to make
        // a copy
        else if (value instanceof Utf8) {
            // Utf8 copy constructor is more efficient than converting
            // to string and then back to Utf8
            return ((Utf8) value).toString();
        }
        return value.toString();
    }
}
