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
import org.sv.flexobject.schema.DataTypes;

public class ObjectNodeWriter {
    private final RecordConsumer recordConsumer;
    private final GroupType schema;

    public ObjectNodeWriter(RecordConsumer recordConsumer, GroupType schema) {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
    }

    public void write(ObjectNode group) throws JsonParquetException {
        this.recordConsumer.startMessage();
        this.writeObject(group, this.schema);
        this.recordConsumer.endMessage();
    }

    private void writeChildObject(ObjectNode group, GroupType type) throws JsonParquetException {
        this.recordConsumer.startGroup();
        this.writeObject(group, type);
        this.recordConsumer.endGroup();
    }

    private void writeObject(ObjectNode group, GroupType type) throws JsonParquetException {
        int fieldCount = type.getFieldCount();

        for(int field = 0; field < fieldCount; ++field) {
            Type fieldType = type.getType(field);
            String fieldName = fieldType.getName();
            JsonNode fieldNode = group.get(fieldName);
            if (fieldNode != null) {
                this.recordConsumer.startField(fieldName, field);

                if (fieldType.getRepetition() == Type.Repetition.REPEATED)
                    writeRepeated(fieldNode, type, fieldType);
                else
                    writeSingle(fieldNode, type, fieldType);

                this.recordConsumer.endField(fieldName, field);
            }
        }

    }

    private void writeSingle(JsonNode fieldNode, GroupType type, Type fieldType) throws JsonParquetException {
        if (fieldType.isPrimitive()) {
            writeValue(fieldNode, (PrimitiveType) fieldType);
        } else {
            writeChildObject((ObjectNode) fieldNode, fieldType.asGroupType());
            //TODO check for "elements" subfield and in this case delegate arrayNode in that subfield
        }
    }

    private void writeValue(JsonNode fieldNode, PrimitiveType fieldType) throws JsonParquetException {
        if (fieldNode.isObject()){
            if (fieldType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY){
                try {
                    recordConsumer.addBinary(Binary.fromString(DataTypes.stringConverter(fieldNode)));
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
                        recordConsumer.addBinary(Binary.fromString(DataTypes.stringConverter(valueNode)));
                        break;
                    case FLOAT:
                        recordConsumer.addFloat(DataTypes.float64Converter(valueNode).floatValue());
                        break;
                    case INT32:
                        recordConsumer.addInteger(DataTypes.int32Converter(valueNode));
                        break;
                    case INT64:
                    case INT96:
                        recordConsumer.addLong(DataTypes.int64Converter(valueNode));
                        break;
                    case DOUBLE:
                        recordConsumer.addDouble(DataTypes.float64Converter(valueNode));
                        break;
                    case BOOLEAN:
                        recordConsumer.addBoolean(DataTypes.boolConverter(valueNode));
                        break;
                }
            } catch (Exception e) {
                throw new JsonParquetException("Failed to convert value to output type + " + fieldType.getPrimitiveTypeName(), e, fieldNode, fieldType.getName());
            }
        }
    }

    private void writeRepeated(JsonNode jsonNode, GroupType type, Type fieldType) throws JsonParquetException {
        if (jsonNode.isArray()){
            for (JsonNode child : jsonNode)
                writeSingle(child, type, fieldType);
        } else
            writeSingle(jsonNode, type, fieldType);
    }

}
