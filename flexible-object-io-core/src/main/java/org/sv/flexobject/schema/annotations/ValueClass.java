package org.sv.flexobject.schema.annotations;

import org.sv.flexobject.Streamable;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ValueClass {
    Class<? extends Streamable> valueClass();
}
