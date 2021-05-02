package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.log4j.Logger;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedGroupConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedPrimitiveConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedRepeatedConverter;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaException;

public class StreamableConverter extends SchemedGroupConverter<StreamableWithSchema> {
    static Logger logger = Logger.getLogger(StreamableConverter.class);

    Schema internalSchema;

    public StreamableConverter(GroupType schema, GroupType fileSchema, Class <? extends StreamableWithSchema> dataClass) {
        super(schema, fileSchema, dataClass);
    }

    public StreamableConverter(SchemedGroupConverter parent, String parentName, GroupType type, GroupType fileSchema, Class <? extends StreamableWithSchema> dataClass) {
        super(parent, parentName, type, fileSchema, dataClass);
    }

    @Override
    protected SchemedPrimitiveConverter<StreamableWithSchema> newPrimitiveConverter(PrimitiveType type) {
        return new PrimitiveFieldConverter(type);
    }

    @Override
    protected SchemedGroupConverter<StreamableWithSchema> newGroupConverter(String parentName, GroupType type, GroupType fileSchema) {
        Class <? extends StreamableWithSchema> childClass;

        if (type == null){
            throw new RuntimeException("Cannot create group converter for field " + parentName + ": message type is unknown.");
        }
        if (getInstanceClass() == null){
            throw new RuntimeException("Cannot create group converter for field " + parentName + ": instance class is not set.");
        }
        try {
            childClass = internalSchema.getDescriptor(type.getName()).getSubschema();
            if (childClass == null){
                logger.warn("Unknown schema for field " + type.getName() + " in " + getInstanceClass().getName());
                return NopConverter.getInstance();
            }
        } catch (Exception e) {
            logger.warn("Unknown schema for field " + type.getName() + " in " + getInstanceClass().getName(), e);
            return NopConverter.getInstance();
        }
        return new StreamableConverter(this, parentName, type, fileSchema, childClass);
    }

    @Override
    protected SchemedRepeatedConverter newRepeatedConverter(String parentName, GroupType type, GroupType fileSchema) {
        return new RepeatedConverter(this, parentName, type, fileSchema, getInstanceClass());
    }

    @Override
    protected void addChildGroup(String name, StreamableWithSchema child) {
        ParquetReadSupport.setField(current, name, child);
    }

    @Override
    public void setInstanceClass(Class<? extends StreamableWithSchema> instanceClass) {
        super.setInstanceClass(instanceClass);
        if (instanceClass != null)
            internalSchema = Schema.getRegisteredSchema(instanceClass);
    }
}
