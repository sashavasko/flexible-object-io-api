package org.sv.flexobject;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StreamableWithSchemaTest extends AbstractBenchmark {

    public static class TestData extends StreamableWithSchema<TestData.FIELDS> {
        protected Integer int32Field;
        private Integer int32FieldGeneric;
        private Integer int32FieldGenericSetter;
        Long int64Field;
        String stringField;
        JsonNode jsonField;
        Boolean booleanField;
        Date dateField;
        Timestamp timestampField;

        public TestData() {
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

            FIELDS(DataTypes type, FunctionWithException<TestData, Object,Exception> getter, BiConsumerWithException<TestData, Object,Exception> setter) {
                descriptor = new FieldDescriptor(name(), type, getter, setter, ordinal());
            }

            FIELDS(DataTypes type) {
                descriptor = new FieldDescriptor(TestData.class, name(), type, ordinal());
            }

            FIELDS(DataTypes type, FunctionWithException<TestData, Object,Exception> getter) {
                descriptor = new FieldDescriptor(TestData.class, name(), type, getter, ordinal());
            }

            public FieldDescriptor getDescriptor() {
                return descriptor;
            }
        }

    }

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void schemaRegistered() {
        assertNotNull(SchemaRegistry.getInstance().getParamNamesXref(TestData.class.getName()));
    }


    // There really is no difference as far as what is used for getters/setters
//    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @Test
    public void direct() throws Exception {
        TestData data = new TestData();
        data.int32Field = 777;

        assertEquals(777, (int)data.int32Field);
    }

//   @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @Test
    public void fullyExplicit() throws Exception {
        TestData data = new TestData();
        data.set(TestData.FIELDS.int32Field, 777);

        assertEquals(777, data.get(TestData.FIELDS.int32Field));
    }

//    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @Test
    public void genericSetter() throws Exception {
        TestData data = new TestData();
        data.set(TestData.FIELDS.int32FieldGenericSetter, 777);

        assertEquals(777, data.get(TestData.FIELDS.int32FieldGenericSetter));
    }

//    @BenchmarkOptions(benchmarkRounds = 10000000, warmupRounds = 1)
    @Test
    public void genericSetterAndGetter() throws Exception {
        TestData data = new TestData();
        data.set(TestData.FIELDS.int32FieldGeneric, 777);

        assertEquals(777, data.get(TestData.FIELDS.int32FieldGeneric));
    }
}