package org.sv.flexobject.hadoop.streaming.parquet.read;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public abstract class SchemedRepeatedConverter<T,CT> extends SchemedGroupConverter<T>{

    CT collection;
    Object currentKey;
    Object currentValue;
    boolean commitOnEnd = false;

    public SchemedRepeatedConverter(GroupType schema, GroupType fileSchema, Class instanceClass) {
        super(schema, fileSchema, instanceClass);
    }

    public SchemedRepeatedConverter(SchemedGroupConverter parent, String parentName, GroupType type, GroupType fileSchema, Class instanceClass) {
        super(parent, parentName, type, fileSchema, instanceClass);
    }

    protected void addMapEntry(){
        ((Map) getCollection()).put(currentKey, currentValue);
        currentKey = null;
        currentValue = null;
    }

    protected void consume(ParquetSchema.MapElementFields fieldType, Object value){
        if (getCollection() instanceof Collection)
            ((Collection)getCollection()).add(value);
        else {
            if (fieldType == ParquetSchema.MapElementFields.value) {
                currentValue = value;
                if (currentKey != null) {
                    addMapEntry();
                }
            } else {
                currentKey = value;
                if (currentValue != null){
                    addMapEntry();
                }
            }
        }
    }

    @Override
    protected void createConverters() {
        converters = new Converter[schema.getFieldCount()];
        int i = 0;
        for (Type field : schema.getFields()) {
            Type fileType = fileSchema.containsField(field.getName()) ? fileSchema.getType(field.getName()) : null;
            if (field.isPrimitive()) {
                converters[i] = newPrimitiveConverter(field.asPrimitiveType());
            } else {
                GroupType fileGroupType = fileType != null && !fileType.isPrimitive() ? fileType.asGroupType() : null;
                converters[i] = newGroupConverter(parentName, field.asGroupType(), fileGroupType);
            }
            i++;
        }
    }

    public abstract void commit(CT values);

    @Override
    protected void addChildGroup(String parentName, T child) {
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
        if (commitOnEnd){
            commit(getCollection());
            setCollection(null);
        }
    }

    public SchemedRepeatedConverter<T,CT> setCommitOnEnd() {
        this.commitOnEnd = true;
        return this;
    }

    public void setCollection(CT collection){
        this.collection = collection;
    }

    public CT getCollection() {
        if (collection == null)
            collection = (CT) new ArrayList();
        return collection;
    }

    @Override
    protected SchemedPrimitiveConverter<T> newPrimitiveConverter(PrimitiveType type) {
        return null;
    }

    @Override
    protected SchemedGroupConverter<T> newGroupConverter(String parentName, GroupType type, GroupType fileSchema) {
        return null;
    }

    @Override
    protected SchemedRepeatedConverter newRepeatedConverter(String parentName, GroupType type, GroupType fileSchema) {
        return null;
    }
}
