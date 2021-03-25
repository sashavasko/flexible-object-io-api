package org.sv.flexobject.testdata;

import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.util.BiConsumerWithException;
import org.sv.flexobject.util.FunctionWithException;

import java.sql.Date;
import java.sql.Timestamp;

public class SimpleTestDataWithSchema extends StreamableWithSchema<SimpleTestDataWithSchema.FIELDS> {
        protected Integer int32Field;
        private Integer int32FieldGeneric;
        private Integer int32FieldGenericSetter;
        Long int64Field;
        String stringField;
        JsonNode jsonField;
        Boolean booleanField;
        Date dateField;
        Timestamp timestampField;

        public SimpleTestDataWithSchema() throws NoSuchFieldException, SchemaException {
            super(FIELDS.values());
        }

        public enum FIELDS implements SchemaElement<FIELDS> {

            int32Field(DataTypes.int32, d->d.int32Field, (d, o)->{d.int32Field = (Integer) o;}),
            int32FieldGeneric(DataTypes.int32),
            int32FieldGenericSetter(DataTypes.int32, d -> d.int32FieldGenericSetter),
            int64Field(DataTypes.int64, (d)->d.int64Field, (d,o)->{d.int64Field = (Long) o;}),
            stringField(DataTypes.string, (d)->d.stringField, (d,o)->{d.stringField = (String) o;}),
            jsonField(DataTypes.jsonNode, (d)->d.jsonField, (d,o)->{d.jsonField = (JsonNode) o;}),
            booleanField(DataTypes.bool, (d)->d.booleanField, (d,o)->{d.booleanField = (Boolean) o;}),
            dateField(DataTypes.date, (d)->d.dateField, (d,o)->{d.dateField = (Date) o;}),
            timestampField(DataTypes.timestamp, (d)->d.timestampField, (d,o)->{d.timestampField = (Timestamp) o;});

            protected FieldDescriptor descriptor;

            FIELDS(DataTypes type, FunctionWithException<SimpleTestDataWithSchema, Object,Exception> getter, BiConsumerWithException<SimpleTestDataWithSchema, Object,Exception> setter) {
                descriptor = new FieldDescriptor(name(), type, getter, setter, ordinal());
            }

            FIELDS(DataTypes type) {
                descriptor = new FieldDescriptor(SimpleTestDataWithSchema.class, name(), type, ordinal());
            }

            FIELDS(DataTypes type, FunctionWithException<SimpleTestDataWithSchema, Object,Exception> getter) {
                descriptor = new FieldDescriptor(SimpleTestDataWithSchema.class, name(), type, getter, ordinal());
            }

            public FieldDescriptor getDescriptor() {
                return descriptor;
            }

            @Override
            public void setDescriptor(FieldDescriptor fieldDescriptor) {

            }
        }

}
