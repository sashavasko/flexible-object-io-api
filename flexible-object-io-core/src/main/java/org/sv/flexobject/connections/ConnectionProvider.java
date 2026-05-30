package org.sv.flexobject.connections;

import java.util.Properties;

public interface ConnectionProvider extends Provider {

    default AutoCloseable getConnection(String name, Class<? extends AutoCloseable> connectionType, Properties connectionProperties, Object secret) throws Exception{
        return getConnection(name,connectionProperties,secret);
    }

    AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception;

    Iterable<Class<? extends AutoCloseable>> listConnectionTypes();

    default boolean requiresProperties(){return true;}
}
