package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedPrimitiveConverter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.DataTypes;

import java.io.IOException;
import java.sql.Timestamp;

public class PrimitiveFieldConverter extends SchemedPrimitiveConverter<StreamableWithSchema> {

    public PrimitiveFieldConverter(Type type) {
        super(type);
    }

    private void set(Object value) {
        try {
            getCurrent().set(getType().getName(), value);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void addBinary(Binary value) {
        switch(getOriginalType()){
            case UTF8:
            case ENUM:
                set(value.toStringUsingUTF8());
                break;
            case JSON:
                try {
                    set(MapperFactory.getObjectReader().readTree(value.toStringUsingUTF8()));
                } catch (IOException e) {
                    set(value.toStringUsingUTF8());
                }
                break;
            default:
                set(value.getBytes());
        }
    }

    @Override
    public void addInt(int value) {
        if (getOriginalType() == OriginalType.DATE) {
            try {
                set(DataTypes.dateConverter(value));
            } catch (Exception e) {
                set(value);
            }
        }else
            set(value);
    }

    @Override
    public void addLong(long value) {

        // this shit is still evolving !!!!!!!!! Gosh darn it
        if (getOriginalType() == OriginalType.TIME_MILLIS
                || getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
            set(new Timestamp(value));
        } else if (getOriginalType() == OriginalType.TIME_MICROS
                || getOriginalType() == OriginalType.TIMESTAMP_MICROS) {
            Timestamp timestamp = new Timestamp(value);
            timestamp.setNanos(((int)(value%1000))*999999999);
            set(timestamp);
        } else
            set(value);
    }

    @Override
    public void addBoolean(boolean value) {
        set(value);
    }

    @Override
    public void addDouble(double value) {
        set(value);
    }

    @Override
    public void addFloat(float value) {
        set(value);
    }
}
