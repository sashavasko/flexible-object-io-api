package org.sv.flexobject.rabbit.domain;


import org.sv.flexobject.StreamableWithSchema;

public class PayloadNested extends StreamableWithSchema {

    PayloadSimple simple;
    String stringVal;

    public PayloadNested() {
    }

    public PayloadNested(PayloadSimple simple, String stringVal) {
        this.simple = simple;
        this.stringVal = stringVal;
    }

    public PayloadSimple getSimple() {
        return simple;
    }

    public String getStringVal() {
        return stringVal;
    }
}
