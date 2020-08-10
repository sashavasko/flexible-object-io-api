package org.sv.flexobject.schema.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ArrayField {
    String classFieldName();
    int index();
}
