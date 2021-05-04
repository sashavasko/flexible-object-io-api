package org.sv.flexobject.hadoop.streaming.parquet.read;

import org.apache.log4j.Logger;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SchemedGroupConverter<T> extends GroupConverter {
    static Logger logger = Logger.getLogger(SchemedGroupConverter.class);

    protected SchemedGroupConverter parent = null;
    protected String parentName = null;
    protected T current;
    protected Converter[] converters;

    protected GroupType schema;

    public GroupType getFileSchema() {
        return fileSchema;
    }

    public SchemedGroupConverter<T> setFileSchema(GroupType fileSchema) {
        this.fileSchema = fileSchema;
        return this;
    }

    protected GroupType fileSchema;
    protected Class<? extends T> instanceClass;

    public SchemedGroupConverter(GroupType schema, GroupType fileSchema, Class<? extends T> instanceClass) {
        this(null, null, schema, fileSchema, instanceClass);
    }

    public SchemedGroupConverter(SchemedGroupConverter parent, String parentName, GroupType schema, GroupType fileSchema, Class<? extends T> instanceClass) {
        this.parent = parent;
        this.parentName = parentName;
        this.fileSchema = fileSchema;
        setInstanceClass(instanceClass);
        setSchema(schema);
        createConverters();
    }

    protected void createConverters(){
        if (schema != null) {
            converters = new Converter[schema.getFieldCount()];

            int i = 0;
            for (Type field : schema.getFields()) {

                Type fileField = fileSchema.containsField(field.getName()) ? fileSchema.getType(field.getName()) : null;

                if (field.isPrimitive()) {
                    if (field.isRepetition(Type.Repetition.REPEATED)){
                        logger.info("Creating Non-Compliant List converter for field \n" + field + " and file field \n" + fileField);
                    }else {
                        logger.debug("Creating primitive converter for field \n" + field + " and file field \n" + fileField);
                    }
                    converters[i++] = newPrimitiveConverter(field.asPrimitiveType());
                } else {
                    GroupType groupType = field.asGroupType();
                    GroupType fileGroupType = fileField != null && !fileField.isPrimitive() ? fileField.asGroupType() : null;
                    if (groupType.getOriginalType() == OriginalType.LIST) {
                        if (fileGroupType != null && !fileGroupType.containsField(ParquetSchema.LIST_OBJECT_NAME)){
                            logger.info("Creating Simple List converter for field \n" + field + " and file field \n" + fileGroupType);
                            converters[i] = newRepeatedConverter(field.getName(), groupType, fileGroupType).setCommitOnEnd();
                        } else {
                            logger.info("Creating List converter for field " + field);
                            converters[i] = new SchemedRepeatedOwnerConverter<List>(groupType, fileGroupType, newRepeatedConverter(field.getName(), groupType.getType(0).asGroupType(), fileGroupType.getType(0).asGroupType()), ArrayList.class);
                        }
                    } else if (groupType.getOriginalType() == OriginalType.MAP) {
                        logger.info("Creating Map converter for field " + field);
                        converters[i] = new SchemedRepeatedOwnerConverter<Map>(groupType, fileGroupType, newRepeatedConverter(field.getName(), groupType.getType(0).asGroupType(), fileGroupType.getType(0).asGroupType()), HashMap.class);
                    } else {
                        logger.info("Creating group converter for field " + field);
                        converters[i] = newGroupConverter(field.getName(), groupType, fileGroupType);
                    }

                    i++;
                }
            }
        }

    }

    protected abstract SchemedPrimitiveConverter<T> newPrimitiveConverter(PrimitiveType type);
    protected abstract SchemedGroupConverter<T> newGroupConverter(String parentName, GroupType type, GroupType fileSchema);
    protected abstract SchemedRepeatedConverter newRepeatedConverter(String parentName, GroupType type, GroupType fileSchema);

    protected <O extends T> O newGroupInstance(){
        if (instanceClass == null)
            throw new RuntimeException("Failed to materialize the new Group Instance as Group class is not set");
        try {
            return (O)instanceClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to materialize the group " + instanceClass.getName() + ". Make sure it has public default constructor.", e);
        }
    }
    protected abstract void addChildGroup(String parentName, T child);

    protected void addCurrentToParent(){
        if (parent != null){
            parent.addChildGroup(parentName, current);
        }
    }

    @Override
    public void start() {
        current = newGroupInstance();
        addCurrentToParent();
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
        if (current == null)
            current = newGroupInstance();
        return current;
    }

    public GroupType getSchema() {
        return schema;
    }

    public void setSchema(GroupType schema) {
        this.schema = schema;
    }

    public Class<? extends T> getInstanceClass() {
        return instanceClass;
    }

    public void setInstanceClass(Class<? extends T> instanceClass) {
        this.instanceClass = instanceClass;
    }

    public SchemedGroupConverter getParent() {
        return parent;
    }

    public String getParentName() {
        return parentName;
    }
}
