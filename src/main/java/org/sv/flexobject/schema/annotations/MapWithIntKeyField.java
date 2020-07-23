package org.sv.flexobject.schema.annotations;

import org.sv.flexobject.schema.DataTypes;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface MapWithIntKeyField {
    DataTypes type();
    String classFieldName();
    int key();
}
