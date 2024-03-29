package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.log4j.Logger;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedGroupConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedPrimitiveConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedRepeatedConverter;
import org.sv.flexobject.schema.Schema;

public class RepeatedConverter<CT> extends SchemedRepeatedConverter<Streamable, CT> {
    static Logger logger = Logger.getLogger(RepeatedConverter.class);

    public RepeatedConverter(GroupType schema, GroupType fileSchema, Class instanceClass) {
        super(schema, fileSchema, instanceClass);
    }

    public RepeatedConverter(SchemedGroupConverter parent, String parentName, GroupType type, GroupType fileSchema, Class instanceClass) {
        super(parent, parentName, type, fileSchema, instanceClass);
    }

    @Override
    protected SchemedPrimitiveConverter<Streamable> newPrimitiveConverter(PrimitiveType type) {
        return new PrimitiveFieldConverter(type) {
            @Override
            protected void set(Object value) {
                consume(ParquetSchema.MapElementFields.forType(getType()), value);
            }
        };
    }

    @Override
    protected SchemedGroupConverter<Streamable> newGroupConverter(String parentName, GroupType type, GroupType fileSchema) {
        Class elementClass;
        try {
            elementClass = Schema.getRegisteredSchema(getInstanceClass()).getDescriptor(parentName).getSubschema();
        } catch (Exception e) {
            throw new RuntimeException("List element class is unknown (missing ValueClass annotation?) for field " + parentName, e);
        }

        return new StreamableConverter(parent, parentName, type, fileSchema, elementClass) {

            @Override
            protected void addCurrentToParent() {
                consume(ParquetSchema.MapElementFields.value, current);
            }
        };
    }

    @Override
    public void commit(CT values) {
        try {
            ((Streamable)getParent().getCurrentRecord()).set(getParentName(), values);
        } catch (Exception e) {
            logger.error("Failed to set repeated for " + getParentName() + " in " + getParent().getClass(), e);
            if (e instanceof RuntimeException)
                throw (RuntimeException)e;
            throw new RuntimeException(e);
        }
    }
}
