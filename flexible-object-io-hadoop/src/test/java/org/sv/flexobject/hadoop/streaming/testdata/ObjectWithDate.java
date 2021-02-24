package org.sv.flexobject.hadoop.streaming.testdata;

import org.sv.flexobject.StreamableWithSchema;

import java.sql.Date;

public class ObjectWithDate extends StreamableWithSchema {
    Date dateField;

    public static ObjectWithDate random() {
        ObjectWithDate value = new ObjectWithDate();
        value.dateField = new Date(System.currentTimeMillis());
        return value;
    }
}
