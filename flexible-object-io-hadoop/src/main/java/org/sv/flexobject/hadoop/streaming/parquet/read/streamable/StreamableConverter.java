package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.log4j.Logger;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedGroupConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedPrimitiveConverter;
import org.sv.flexobject.schema.SchemaException;

public class StreamableConverter extends SchemedGroupConverter<StreamableWithSchema> {
    Logger logger = Logger.getLogger(StreamableConverter.class);

    Class <? extends StreamableWithSchema> dataClass;

    public StreamableConverter(GroupType schema) {
        super(schema);
        if (schema != null) {
            dataClass = ParquetSchema.forType(schema);
            if (dataClass == null) {
                throw new RuntimeException("Unable to load class for schema " + schema);
            }
        }
    }

    public StreamableConverter(SchemedGroupConverter parent, String parentName, GroupType type, Class <? extends StreamableWithSchema> dataClass) {
        super(parent, parentName, type);
        this.dataClass = dataClass;
    }

    @Override
    protected SchemedPrimitiveConverter<StreamableWithSchema> newPrimitiveConverter(PrimitiveType type) {
        return new PrimitiveFieldConverter(type);
    }

    @Override
    protected SchemedGroupConverter<StreamableWithSchema> newGroupConverter(String parentName, GroupType type) {
        Class <? extends StreamableWithSchema> dataClass;
        try {
            dataClass = getCurrentRecord().getSchema().getDescriptor(type.getName()).getSubschema();
        } catch (Exception e) {
            logger.warn("Unknown schema for field " + type.getName() + " in " + getCurrentRecord().getClass().getName());
            return NopConverter.getInstance();
        }
        return new StreamableConverter(this, parentName, type, dataClass);
    }

    protected StreamableWithSchema newGroupInstance(){
        try {
            return dataClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to materialize the group " + dataClass.getName());
        }
    }

    @Override
    protected void addChildGroup(String name, StreamableWithSchema child) {
    }

}
