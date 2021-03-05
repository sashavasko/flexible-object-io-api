package org.sv.flexobject.sql.providers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Properties;

public class UnPooledConnectionProvider implements ConnectionProvider {

    static Logger logger = LogManager.getLogger(UnPooledConnectionProvider.class);

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(Connection.class, UnPooledSqlConnection.class);
    }

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {

        Class.forName(connectionProperties.getProperty("driverClassName"));
        String url = connectionProperties.getProperty("url");

        if (secret == null || connectionProperties.get("password") != null)
            return DriverManager.getConnection(url, connectionProperties);

        Properties propsWithPassword = new Properties();
        propsWithPassword.putAll(connectionProperties);
        propsWithPassword.put("password", secret.toString());

        logger.info("connecting as " + connectionProperties.getProperty("username") + " to: " + url);

        return DriverManager.getConnection(url, propsWithPassword);
    }
}
