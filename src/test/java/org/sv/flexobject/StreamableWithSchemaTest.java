package org.sv.flexobject;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Before;
import org.junit.Test;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaRegistry;
import org.sv.flexobject.util.BiConsumerWithException;
import org.sv.flexobject.util.FunctionWithException;

import java.sql.Date;
import java.sql.Timestamp;

import static org.junit.Assert.assertNotNull;

public class StreamableWithSchemaTest {

    public static class TestData extends StreamableWithSchema<TestData.FIELDS> {

        public TestData() {
            super(FIELDS.values());
        }

        public enum FIELDS implements SchemaElement<FIELDS> {

            int32Field(DataTypes.int32, (d)->d.int32Field, (d, o)->{d.int32Field = (Integer) o;}),
            int64Field(DataTypes.int64, (d)->d.int64Field, (d,o)->{d.int64Field = (Long) o;}),
            stringField(DataTypes.string, (d)->d.stringField, (d,o)->{d.stringField = (String) o;}),
            jsonField(DataTypes.jsonNode, (d)->d.jsonField, (d,o)->{d.jsonField = (JsonNode) o;}),
            booleanField(DataTypes.bool, (d)->d.booleanField, (d,o)->{d.booleanField = (Boolean) o;}),
            dateField(DataTypes.date, (d)->d.dateField, (d,o)->{d.dateField = (Date) o;}),
            timestampField(DataTypes.timestamp, (d)->d.timestampField, (d,o)->{d.timestampField = (Timestamp) o;});

            protected FieldDescriptor descriptor;

            FIELDS(DataTypes type, FunctionWithException<TestData, Object,Exception> getter, BiConsumerWithException<TestData, Object,Exception> setter) {
                descriptor = new FieldDescriptor(name(), type, getter, setter, ordinal());
            }

            public FieldDescriptor getDescriptor() {
                return descriptor;
            }
        }

        Integer int32Field;
        Long int64Field;
        String stringField;
        JsonNode jsonField;
        Boolean booleanField;
        Date dateField;
        Timestamp timestampField;
    }

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void schemaRegistered() {
        assertNotNull(SchemaRegistry.getInstance().getParamNamesXref(TestData.class.getName()));

//        System.out.println(SchemaRegistry.getInstance().getParamNamesXref(TestData.class.getName()));
    }
}