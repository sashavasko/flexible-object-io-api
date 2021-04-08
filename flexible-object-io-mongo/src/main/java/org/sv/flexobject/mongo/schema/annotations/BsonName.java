package org.sv.flexobject.mongo.schema.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface BsonName {
    String name();
}
