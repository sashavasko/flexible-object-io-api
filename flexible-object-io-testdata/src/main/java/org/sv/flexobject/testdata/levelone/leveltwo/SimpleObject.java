package org.sv.flexobject.testdata.levelone.leveltwo;

import org.sv.flexobject.StreamableWithSchema;

public class SimpleObject<SELF extends SimpleObject> extends StreamableWithSchema {
    public int intField;

    public SELF randomInit(){
        intField = (int)Math.round(Math.random() * Integer.MAX_VALUE);
        return (SELF)this;
    }

    public static SimpleObject random() {
        SimpleObject instance = new SimpleObject();
        return instance.randomInit();
    }
}