package org.sv.flexobject.hadoop.streaming.parquet.read.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.DataTypes;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;

public class ValueNodeConverter  extends PrimitiveConverter {

    ObjectNode current;
    Type type;

    public ValueNodeConverter(Type type) {
        this.type = type;
    }

    private void set(JsonNode node) {
        try {
            current.set(type.getName(), node);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void addBinary(Binary value) {
        switch(type.getOriginalType()){
            case UTF8:
            case ENUM:
                set(JsonNodeFactory.instance.textNode(value.toStringUsingUTF8()));
                break;
            case JSON:
                try {
                    set(MapperFactory.getObjectReader().readTree(value.toStringUsingUTF8()));
                } catch (IOException e) {
                    set(JsonNodeFactory.instance.textNode(value.toStringUsingUTF8()));
                }
                break;
            default:
                set(JsonNodeFactory.instance.binaryNode(value.getBytes()));
        }
    }

    @Override
    public void addInt(int value) {
        if (type.getOriginalType() == OriginalType.DATE) {
            try {
                Date date = DataTypes.dateConverter(value);
                set(JsonOutputAdapter.dateToJsonNode(date));
            } catch (Exception e) {
                set(JsonNodeFactory.instance.numberNode(value));
            }
        }else
            set(JsonNodeFactory.instance.numberNode(value));
    }

    @Override
    public void addLong(long value) {

        // this shit is still evolving !!!!!!!!! Gosh darn it
        if (type.getOriginalType() == OriginalType.TIME_MILLIS
            || type.getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
            Timestamp timestamp = new Timestamp(value);
            set(JsonOutputAdapter.timestampToJsonNode(timestamp));
        } else if (type.getOriginalType() == OriginalType.TIME_MICROS
            || type.getOriginalType() == OriginalType.TIMESTAMP_MICROS) {
            Timestamp timestamp = new Timestamp(value);
            timestamp.setNanos(((int)(value%1000))*999999999);
            set(JsonOutputAdapter.timestampToJsonNode(timestamp));
        } else
            set(JsonNodeFactory.instance.numberNode(value));
    }

    @Override
    public void addBoolean(boolean value) {
        set(JsonNodeFactory.instance.booleanNode(value));
    }

    @Override
    public void addDouble(double value) {
        set(JsonNodeFactory.instance.numberNode(value));
    }

    @Override
    public void addFloat(float value) {
        set(JsonNodeFactory.instance.numberNode(value));
    }

    public void setCurrent (ObjectNode group){
        current = group;
    }
}
