package org.sv.flexobject.sql.providers;

import java.sql.DriverManager;
import java.util.Properties;

public class UnPooledConnectionProvider implements SqlConnectionProvider {
    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {

        Class.forName(connectionProperties.getProperty("driverClassName"));
        String url = connectionProperties.getProperty("url");

        if (secret == null || connectionProperties.get("password") != null)
            return DriverManager.getConnection(url, connectionProperties);

        Properties propsWithPassword = new Properties();
        propsWithPassword.putAll(connectionProperties);
        propsWithPassword.put("password", secret.toString());

        return DriverManager.getConnection(url, propsWithPassword);
    }
}
