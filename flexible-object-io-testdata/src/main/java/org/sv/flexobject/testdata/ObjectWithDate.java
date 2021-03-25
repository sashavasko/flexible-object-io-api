package org.sv.flexobject.testdata;

import org.sv.flexobject.StreamableWithSchema;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public class ObjectWithDate extends StreamableWithSchema {
    public Date dateField;
    public LocalDate localDateField;
    public Timestamp timestampField;

    public static ObjectWithDate random() {
        ObjectWithDate value = new ObjectWithDate();
        value.localDateField = LocalDate.now();
        value.dateField = new Date(System.currentTimeMillis()+20);
        value.timestampField = new Timestamp(System.currentTimeMillis()+40);
        return value;
    }
}
