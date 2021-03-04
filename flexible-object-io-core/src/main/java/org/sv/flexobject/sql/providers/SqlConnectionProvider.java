package org.sv.flexobject.sql.providers;

import org.sv.flexobject.connections.ConnectionProvider;

import java.sql.Connection;
import java.util.Arrays;

public interface SqlConnectionProvider extends ConnectionProvider {

    @Override
    default Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(Connection.class);
    }
}
