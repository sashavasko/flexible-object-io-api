package org.sv.flexobject.hadoop.streaming.parquet.read;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;

public class StreamableConverter extends GroupConverter {
    private StreamableConverter parent = null;
    private int parentIndex = 0;
    private StreamableWithSchema current;
    private Converter[] converters;

    Class<? extends StreamableWithSchema> dataClass = null;

    public StreamableConverter(Class<? extends StreamableWithSchema> dataClass) {
        this.dataClass = dataClass;
        SchemaElement[] fields = Schema.getRegisteredSchema(dataClass).getFields();
        int fieldCount = fields.length;
        converters = new Converter[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            FieldDescriptor fieldDescriptor = fields[i].getDescriptor();

            // TODO add support for Lists and Maps !!!

            if (fieldDescriptor.getType() != DataTypes.jsonNode)
                converters[i] =  new PrimitiveFieldConverter(fieldDescriptor);
            else
                converters[i] = new StreamableConverter(this, i);
        }
    }

    public StreamableConverter(StreamableConverter parent, int parentIndex) {
//        this(parent.example.instantiateSubgroup(parentIndex));
        this.parent = parent;
        this.parentIndex = parentIndex;
    }

    protected StreamableWithSchema newGroupInstance() throws IllegalAccessException, InstantiationException {
        return dataClass.newInstance();
    }

    @Override
    public void start() {
//        if (parent == null)
//            current = newGroupInstance();
//        else {
//            current = (StreamableWithSchema) parent.getCurrentRecord().addGroup(parentIndex);
//            if (current == null){
//                throw new RuntimeException(getClass().getName() + " Failed to instantiate new Group with parentIndex " + parentIndex + ". Parent Group class is " + parent.getCurrentRecord().getClass().getName());
//            }
//            current.clear();
//        }
//
//        for (Converter converter : converters) {
//            if (converter instanceof GenericPrimitiveConverter)
//                ((GenericPrimitiveConverter)converter).setCurrent(current);
//        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void end() {}

//    public ObjectNode getCurrentRecord() {
//        return current;
//    }
}
