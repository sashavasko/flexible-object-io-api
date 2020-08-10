package org.sv.flexobject.schema.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface EnumSetField {
    Class<? extends Enum> enumClass();
    String emptyValue();
}
