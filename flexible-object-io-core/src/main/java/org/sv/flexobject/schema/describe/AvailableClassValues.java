package org.sv.flexobject.schema.describe;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface AvailableClassValues {
    Class<?> extend() default Void.class;
    Class<?> implement() default Void.class;
    String namespace() default "com.carfax";
}
