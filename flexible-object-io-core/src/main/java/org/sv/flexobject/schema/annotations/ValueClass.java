package org.sv.flexobject.schema.annotations;

import org.sv.flexobject.StreamableWithSchema;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ValueClass {
    Class<? extends StreamableWithSchema> valueClass();
}
