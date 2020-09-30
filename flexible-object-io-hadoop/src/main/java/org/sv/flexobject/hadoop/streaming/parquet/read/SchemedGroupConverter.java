package org.sv.flexobject.hadoop.streaming.parquet.read;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public abstract class SchemedGroupConverter<T> extends GroupConverter {
    private SchemedGroupConverter parent = null;
    private String parentName = null;
    private T current;
    private Converter[] converters;

    GroupType schema;

    public SchemedGroupConverter(GroupType schema) {
        this.schema = schema;
        converters = new Converter[schema.getFieldCount()];

        int i = 0;
        for (Type field : schema.getFields()) {
            if (field.isPrimitive())
                converters[i++] = newPrimitiveConverter(field.asPrimitiveType());
            else
                converters[i++] = newGroupConverter(field.getName(), field.asGroupType());
        }
    }

    public SchemedGroupConverter(SchemedGroupConverter parent, String parentName, GroupType type) {
        this(type);
        this.parent = parent;
        this.parentName = parentName;
    }

    protected abstract SchemedPrimitiveConverter<T> newPrimitiveConverter(PrimitiveType type);
    protected abstract SchemedGroupConverter<T> newGroupConverter(String parentName, GroupType type);
    protected abstract T newGroupInstance();
    protected abstract void addChildGroup(String parentName, T child);

    @Override
    public void start() {
        current = newGroupInstance();
        if (parent != null){
            parent.addChildGroup(parentName, current);
        }

        for (Converter converter : converters) {
            if (converter != null) {
                if (converter instanceof SchemedPrimitiveConverter)
                    ((SchemedPrimitiveConverter) converter).setCurrent(current);
            }
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void end() {}

    public T getCurrentRecord() {
        return current;
    }

    public GroupType getSchema() {
        return schema;
    }
}
