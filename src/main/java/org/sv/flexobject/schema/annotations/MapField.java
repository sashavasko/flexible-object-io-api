package org.sv.flexobject.schema.annotations;

import org.sv.flexobject.schema.DataTypes;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface MapField {
    DataTypes type();
    String classFieldName();
    String key();
}
