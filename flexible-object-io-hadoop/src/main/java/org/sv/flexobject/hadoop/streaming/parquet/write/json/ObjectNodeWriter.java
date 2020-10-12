package org.sv.flexobject.hadoop.streaming.parquet.write.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.sv.flexobject.hadoop.streaming.parquet.write.ParquetWriteException;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedWriter;
import org.sv.flexobject.schema.DataTypes;

import java.util.Iterator;
import java.util.Map;

public class ObjectNodeWriter extends SchemedWriter<ObjectNode, JsonNode> {

    public ObjectNodeWriter(RecordConsumer recordConsumer, GroupType schema) {
        super(recordConsumer, schema);
    }

    @Override
    public JsonNode getGroupValue(ObjectNode group, String fieldName) {
        return group.get(fieldName);
    }

    public void writeRepeated(JsonNode jsonNode, Type fieldType) throws ParquetWriteException {
        if (jsonNode.isArray()){
            for (JsonNode child : jsonNode)
                writeListElement(child, fieldType);
        } else
            writeListElement(jsonNode, fieldType);
    }

    @Override
    public void writeMap(JsonNode o, Type fieldType) throws ParquetWriteException {
        System.out.println("writeMap for " + o.getClass() + " field Type:" + fieldType);
        if (o.isObject()){
            for (Iterator<Map.Entry<String, JsonNode>> it = o.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> child = it.next();
                writeMapElement(child.getKey(), child.getValue(), fieldType);
            }
        } else
            throw new ParquetWriteException("Attempt to write ValueNode to MAP " + fieldType);
    }

    public void writeValue(JsonNode fieldNode, PrimitiveType fieldType) throws JsonParquetException {
        if (fieldNode.isObject()){
            if (fieldType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY){
                try {
                    getRecordConsumer().addBinary(Binary.fromString(DataTypes.stringConverter(fieldNode)));
                } catch (Exception e) {
                    throw new JsonParquetException("Failed to convert JsonNode to BINARY representation", e, fieldNode, fieldType.getName());
                }
            } else {
                throw new JsonParquetException("Can only write object node into BINARY primitive field", fieldNode, fieldType.getName());
            }
        } else if (fieldNode.isArray()){
            ArrayNode array = (ArrayNode) fieldNode;

            if (fieldType.getRepetition() != Type.Repetition.REPEATED && array.size() > 1)
                throw new JsonParquetException("Attempting to write array node into non REPEATED primitive field", fieldNode, fieldType.getName());

            for (JsonNode node : array){
                writeValue(node, fieldType);
            }
        } else {
            ValueNode valueNode = (ValueNode) fieldNode;
            try {
                switch(fieldType.getPrimitiveTypeName()) {
                    case BINARY:
                        getRecordConsumer().addBinary(Binary.fromString(DataTypes.stringConverter(valueNode)));
                        break;
                    case FLOAT:
                        getRecordConsumer().addFloat(DataTypes.float64Converter(valueNode).floatValue());
                        break;
                    case INT32:
                        getRecordConsumer().addInteger(DataTypes.int32Converter(valueNode));
                        break;
                    case INT64:
                    case INT96:
                        getRecordConsumer().addLong(DataTypes.int64Converter(valueNode));
                        break;
                    case DOUBLE:
                        getRecordConsumer().addDouble(DataTypes.float64Converter(valueNode));
                        break;
                    case BOOLEAN:
                        getRecordConsumer().addBoolean(DataTypes.boolConverter(valueNode));
                        break;
                }
            } catch (Exception e) {
                throw new JsonParquetException("Failed to convert value to output type + " + fieldType.getPrimitiveTypeName(), e, fieldNode, fieldType.getName());
            }
        }
    }

}
