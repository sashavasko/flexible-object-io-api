package org.sv.flexobject.hadoop.streaming.parquet.read.json;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

public class ObjectNodeConverter extends GroupConverter {
    private ObjectNodeConverter parent = null;
    private String parentName = null;
    private ObjectNode current;
    private Converter[] converters;

    GroupType schema;

    public ObjectNodeConverter(GroupType schema) {
        this.schema = schema;
        converters = new Converter[schema.getFieldCount()];

        int i = 0;
        for (Type field : schema.getFields()) {
            if (field.isPrimitive())
                converters[i++] =  new ValueNodeConverter(field);
            else
                converters[i++] = new ObjectNodeConverter(this, field.getName(), field.asGroupType());
        }
    }

    public ObjectNodeConverter(ObjectNodeConverter parent, String parentName, GroupType type) {
        this(type);
        this.parent = parent;
        this.parentName = parentName;
    }

    protected ObjectNode newGroupInstance(){
        return JsonNodeFactory.instance.objectNode();
    }

    @Override
    public void start() {
        current = newGroupInstance();
        if (parent != null){
            JsonReadSupport.setJsonNodeWithRepetition(parent.getCurrentRecord(), parentName, current);
        }

        for (Converter converter : converters) {
            if (converter instanceof ValueNodeConverter)
                ((ValueNodeConverter)converter).setCurrent(current);
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void end() {}

    public ObjectNode getCurrentRecord() {
        return current;
    }
}
