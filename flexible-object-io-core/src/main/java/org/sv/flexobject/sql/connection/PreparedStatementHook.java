package org.sv.flexobject.sql.connection;

import java.sql.Connection;
import java.sql.PreparedStatement;

public interface PreparedStatementHook {

    default void onPrepare(Connection connection, String sql){}
    default PreparedStatement onStatementPrepared(Connection connection, String sql, PreparedStatement preparedStatement){
        return preparedStatement;
    }

    default void onExecute(Connection connection, PreparedStatement ps){}
    default void onResult(Connection connection, PreparedStatement ps, Object result){}

}
