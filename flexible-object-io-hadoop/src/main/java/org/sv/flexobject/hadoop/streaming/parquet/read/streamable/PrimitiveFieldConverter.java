package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedPrimitiveConverter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.DataTypes;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class PrimitiveFieldConverter extends SchemedPrimitiveConverter<Streamable> {

    public PrimitiveFieldConverter(Type type) {
        super(type);
    }

    protected void set(Object value) {
        ParquetReadSupport.setField(getCurrent(), getType().getName(), value);
    }

    @Override
    public void addBinary(Binary value) {
        OriginalType originalType = getOriginalType();
        if (originalType == null) {
            set(value.getBytes());
        } else {
            switch (originalType) {
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
        LogicalTypeAnnotation logicalTypeAnnotation = getType().getLogicalTypeAnnotation();
        if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation){
            Timestamp ts = convertTime(value, (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation);
            set(ts);
        } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation){
            Timestamp ts = convertTime(value, (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalTypeAnnotation);
            set(ts);
        }else
            set(value);
    }

    private static Timestamp convertTime(long value, LogicalTypeAnnotation.TimeLogicalTypeAnnotation tsAnnotation) {
        return convertTime(value, tsAnnotation.getUnit(), tsAnnotation.isAdjustedToUTC());
    }
    private static Timestamp convertTime(long value, LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tsAnnotation) {
        return convertTime(value, tsAnnotation.getUnit(), tsAnnotation.isAdjustedToUTC());
    }

    private static Timestamp convertTime(long value, LogicalTypeAnnotation.TimeUnit unit, boolean adjustedToUTC) {
        ZoneOffset offset = OffsetDateTime.now().getOffset();
        long offsetSeconds = adjustedToUTC ? 0 : offset.getTotalSeconds();
//        System.out.println("AdjustedToUTC: " + adjustedToUTC);
        Timestamp ts = switch(unit){
            case MILLIS -> new Timestamp(value + offsetSeconds * 1000);
            case MICROS ->
                    Timestamp.from(Instant.ofEpochSecond(
                            offsetSeconds + (value / 1_000_000),           // Whole seconds
                            (value % 1_000_000) * 1_000  // Remaining nanos
                    ));
            case NANOS ->
                    Timestamp.from(Instant.ofEpochSecond(offsetSeconds, value));
        };
        return ts;
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
