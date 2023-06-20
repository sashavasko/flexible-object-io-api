package org.sv.flexobject.dt.streaming.examples.rabbit.domain;


import org.sv.flexobject.StreamableWithSchema;

public class PayloadSimple extends StreamableWithSchema {
    Integer intVal;
    String stringVal;

    public PayloadSimple() {
    }

    public PayloadSimple(Integer intVal, String stringVal) {
        this.intVal = intVal;
        this.stringVal = stringVal;
    }

    public Integer getIntVal() {
        return intVal;
    }

    public String getStringVal() {
        return stringVal;
    }

    public PayloadSimple inc(){
        intVal++;
        return this;
    }
}
