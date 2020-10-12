package org.sv.flexobject.hadoop.streaming.parquet.write;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;

abstract public class SchemedWriter<GT, VT> {

    private final RecordConsumer recordConsumer;
    private final GroupType schema;

    public SchemedWriter(RecordConsumer recordConsumer, GroupType schema) {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
    }

    public RecordConsumer getRecordConsumer() {
        return recordConsumer;
    }

    public GroupType getSchema() {
        return schema;
    }

    public void write(GT group) throws ParquetWriteException {
        getRecordConsumer().startMessage();
        writeObject(group, getSchema());
        getRecordConsumer().endMessage();
    }

    protected void writeChildObject(GT group, GroupType type) throws ParquetWriteException {
        getRecordConsumer().startGroup();
        if (type.getOriginalType() == OriginalType.LIST)
            writeListSubtree(group, type);
        else if (type.getOriginalType() == OriginalType.MAP)
            writeMapSubtree(group, type);
        else
            writeObject(group, type);
        getRecordConsumer().endGroup();
    }

    protected void writeListSubtree(GT group, GroupType type) throws ParquetWriteException {
        Type fieldType = type.getType(0);
        String fieldName = fieldType.getName();

        // writing special list subgroup!
        getRecordConsumer().startField(fieldName, 0);
        writeRepeated((VT)group, fieldType);
        getRecordConsumer().endField(fieldName, 0);
    }

    protected void writeMapSubtree(GT group, GroupType type) throws ParquetWriteException {
        Type fieldType = type.getType(0);
        String fieldName = fieldType.getName();

        // writing special list subgroup!
        getRecordConsumer().startField(fieldName, 0);
        writeMap((VT)group, fieldType);
        getRecordConsumer().endField(fieldName, 0);
    }


    protected VT getFieldValue(GT group, String fieldName) throws ParquetWriteException{
        try {
            return getGroupValue(group, fieldName);
        } catch (Exception e) {
            throw new ParquetWriteException("Failed to get subgroup object", e, fieldName);
        }
    }

    protected void writeField(int field, VT value, GroupType type) throws ParquetWriteException {
        if (value != null) {
            Type fieldType = type.getType(field);
            String fieldName = fieldType.getName();
            getRecordConsumer().startField(fieldName, field);
            writeSingle(value, fieldType);
            getRecordConsumer().endField(fieldName, field);
        }
    }

    protected void writeObject(GT group, GroupType type) throws ParquetWriteException {
        int fieldCount = type.getFieldCount();

        for(int field = 0; field < fieldCount; ++field) {
            writeField(field, getFieldValue(group, type.getType(field).getName()), type);
        }

    }

    protected void writeSingle(VT fieldNode, Type fieldType) throws ParquetWriteException {
        if (fieldType.isPrimitive()) {
            writeValue(fieldNode, (PrimitiveType) fieldType);
        } else {
            writeChildObject((GT) fieldNode, fieldType.asGroupType());
            //TODO check for "elements" subfield and in this case delegate arrayNode in that subfield
        }
    }

    protected void writeListElement(VT fieldNode, Type fieldType) throws ParquetWriteException {
        System.out.println("writeListElement for :" + fieldNode + " Field type : " + fieldType);
        // Field Type is: repeated group list { optional element }
        GroupType listType = fieldType.asGroupType();
        getRecordConsumer().startGroup();

        writeField(0, fieldNode, listType);
        getRecordConsumer().endGroup();
    }

    protected void writeMapElement(Object key, Object value, Type fieldType) throws ParquetWriteException {
        System.out.println("writeMapElement for : (" + key + "," + value + ") Field type : " + fieldType);
        // Field Type is: repeated group key_value { required binary key ; optional value; }
        GroupType keyValueType = fieldType.asGroupType();
        getRecordConsumer().startGroup();
        if (key != null) {
            writeField(ParquetSchema.MapElementFields.key.ordinal(), (VT) key, keyValueType);
            writeField(ParquetSchema.MapElementFields.value.ordinal(), (VT) value, keyValueType);
        }
        getRecordConsumer().endGroup();
    }

    public abstract VT getGroupValue(GT group, String fieldName) throws Exception;

    public abstract void writeRepeated(VT value, Type fieldType) throws ParquetWriteException;
    public abstract void writeMap(VT value, Type fieldType) throws ParquetWriteException;

    public abstract void writeValue(VT value, PrimitiveType fieldType) throws ParquetWriteException;

}
