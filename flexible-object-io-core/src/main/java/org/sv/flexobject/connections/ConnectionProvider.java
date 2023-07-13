package org.sv.flexobject.connections;

import java.util.Properties;

public interface ConnectionProvider extends Provider {

    AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception;

    Iterable<Class<? extends AutoCloseable>> listConnectionTypes();

    default boolean requiresProperties(){return true;}
}
