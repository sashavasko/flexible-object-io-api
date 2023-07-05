package org.sv.flexobject.testdata;

import org.sv.flexobject.StreamableWithSchema;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class ObjectWithDate extends StreamableWithSchema {
    public Date dateField;
    public LocalDate localDateField;
    public Timestamp timestampField;

    public static ObjectWithDate random() {
        ObjectWithDate value = new ObjectWithDate();
        value.localDateField = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toLocalDate();
        value.dateField = new Date(((System.currentTimeMillis()+20000)%1000)*1000);
        value.timestampField = new Timestamp(System.currentTimeMillis()+40);
        return value;
    }
}
