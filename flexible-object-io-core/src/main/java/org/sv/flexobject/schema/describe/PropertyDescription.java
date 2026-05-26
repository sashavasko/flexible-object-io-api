package org.sv.flexobject.schema.describe;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface PropertyDescription {
    String displayName();
    String description();
}
