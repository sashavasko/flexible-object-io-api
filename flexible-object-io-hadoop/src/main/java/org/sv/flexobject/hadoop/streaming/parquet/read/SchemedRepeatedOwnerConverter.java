package org.sv.flexobject.hadoop.streaming.parquet.read;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

public class SchemedRepeatedOwnerConverter<CT> extends SchemedGroupConverter<CT>{

    SchemedRepeatedConverter listConverter;

    public SchemedRepeatedOwnerConverter(GroupType schema, GroupType fileSchema, SchemedRepeatedConverter listConverter, Class<? extends CT> collectionClass) {
        super(schema, fileSchema, collectionClass);
        this.listConverter = listConverter;
    }

    public SchemedRepeatedOwnerConverter(SchemedGroupConverter parent, String parentName, GroupType type, GroupType fileSchema, Class instanceClass) {
        super(parent, parentName, type, fileSchema, instanceClass);
    }

    @Override
    protected void createConverters() {
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return listConverter;
    }

    @Override
    protected void addChildGroup(String parentName, CT child) {
    }

    @Override
    public void start() {
        try {
            current = getInstanceClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        listConverter.setCollection(current);
    }

    @Override
    public void end() {
        listConverter.commit(current);
    }

    @Override
    protected SchemedPrimitiveConverter<CT> newPrimitiveConverter(PrimitiveType type) {
        return null;
    }

    @Override
    protected SchemedGroupConverter<CT> newGroupConverter(String parentName, GroupType type, GroupType fileSchema) {
        return null;
    }

    @Override
    protected SchemedRepeatedConverter newRepeatedConverter(String parentName, GroupType type, GroupType fileSchema) {
        return null;
    }
}
