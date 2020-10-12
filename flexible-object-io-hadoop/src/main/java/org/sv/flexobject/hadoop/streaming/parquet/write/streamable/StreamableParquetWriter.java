package org.sv.flexobject.hadoop.streaming.parquet.write.streamable;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.write.ParquetWriteException;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedWriter;
import org.sv.flexobject.schema.DataTypes;

import java.util.List;
import java.util.Map;

public class StreamableParquetWriter extends SchemedWriter<StreamableWithSchema, Object> {
    public StreamableParquetWriter(RecordConsumer recordConsumer, GroupType schema) {
        super(recordConsumer, schema);
    }

    @Override
    public Object getGroupValue(StreamableWithSchema group, String fieldName) throws Exception {
        return group.get(fieldName);
    }

    public void writeValue(Object value, PrimitiveType fieldType) throws ParquetWriteException {
        if (value instanceof StreamableWithSchema){
            if (fieldType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY){
                try {
                    getRecordConsumer().addBinary(Binary.fromString(DataTypes.stringConverter(value)));
                } catch (Exception e) {
                    throw new ParquetWriteException("Failed to convert JsonNode to BINARY representation", e, fieldType.getName());
                }
            } else {
                throw new ParquetWriteException("Can only write object node into BINARY primitive field", fieldType.getName());
            }
        } else if (value.getClass().isArray()){
// TODO
            //            ArrayNode array = (ArrayNode) value;
//
//            if (fieldType.getRepetition() != Type.Repetition.REPEATED && array.size() > 1)
//                throw new ParquetWriteException("Attempting to write array node into non REPEATED primitive field", fieldType.getName());
//
//            for (JsonNode node : array){
//                writeValue(node, fieldType);
//            }
        } else {
            try {
                switch(fieldType.getPrimitiveTypeName()) {
                    case BINARY:
                        getRecordConsumer().addBinary(Binary.fromString(DataTypes.stringConverter(value)));
                        break;
                    case FLOAT:
                        getRecordConsumer().addFloat(DataTypes.float64Converter(value).floatValue());
                        break;
                    case INT32:
                        getRecordConsumer().addInteger(DataTypes.int32Converter(value));
                        break;
                    case INT64:
                    case INT96:
                        getRecordConsumer().addLong(DataTypes.int64Converter(value));
                        break;
                    case DOUBLE:
                        getRecordConsumer().addDouble(DataTypes.float64Converter(value));
                        break;
                    case BOOLEAN:
                        getRecordConsumer().addBoolean(DataTypes.boolConverter(value));
                        break;
                }
            } catch (Exception e) {
                throw new ParquetWriteException("Failed to convert value of class " + value.getClass() + " to output type " + fieldType.getPrimitiveTypeName(), e, fieldType.getName());
            }
        }
    }

    public void writeRepeated(Object o, Type fieldType) throws ParquetWriteException {
        System.out.println("writeRepeated for " + o.getClass() + " field Type:" + fieldType);
        if (o.getClass().isArray()){
            for (Object child : ((Object[])o))
                writeListElement(child, fieldType);
        } else if (List.class.isAssignableFrom(o.getClass())){
            for (Object child : (List)o)
                writeListElement(child, fieldType);
        } else
            writeListElement(o, fieldType);
    }

    @Override
    public void writeMap(Object o, Type fieldType) throws ParquetWriteException {
        System.out.println("writeMap for " + o.getClass() + " field Type:" + fieldType);
        if (Map.class.isAssignableFrom(o.getClass())){
            for (Object child : ((Map)o).entrySet())
                writeMapElement(((Map.Entry)child).getKey(), ((Map.Entry)child).getValue(), fieldType);
        } else
            throw new ParquetWriteException("Attempt to write object of class : " + o.getClass() + " to MAP " + fieldType);
    }
}
