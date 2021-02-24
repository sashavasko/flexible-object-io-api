package org.sv.flexobject.hadoop.streaming.testdata.levelone.leveltwo;

import org.sv.flexobject.StreamableWithSchema;

public class SimpleObject extends StreamableWithSchema {
    int intField;

    public static SimpleObject random() {
        SimpleObject data = new SimpleObject();
        data.intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        return data;
    }
}
