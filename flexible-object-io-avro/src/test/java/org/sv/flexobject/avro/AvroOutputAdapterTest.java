package org.sv.flexobject.avro;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.testdata.ObjectWithDate;

import java.sql.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroOutputAdapterTest {

    @Test
    public void setDate() throws Exception {
        ObjectWithDate value = ObjectWithDate.random();

        GenericRecord record = AvroOutputAdapter.produce(AvroSchema.forClass(ObjectWithDate.class), value::save);

        Date valueDate = (Date) value.get("dateField");
        assertEquals((int)valueDate.toLocalDate().toEpochDay(), record.get("dateField"));
    }
}