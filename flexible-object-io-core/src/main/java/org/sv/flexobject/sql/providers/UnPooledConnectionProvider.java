package org.sv.flexobject.sql.providers;

import org.apache.commons.lang3.StringUtils;
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

        String driverClassName = connectionProperties.getProperty("driverClassName");
        if (StringUtils.isNotBlank(driverClassName))
            Class.forName(connectionProperties.getProperty("driverClassName"));
        // otherwise DriverManager will load driver from
        // those listed in jdbc.drivers system property

        Properties amendedProps = new Properties();
        amendedProps.putAll(connectionProperties);

        String url = connectionProperties.getProperty("url");
        String user = connectionProperties.getProperty("user");
        if (StringUtils.isBlank(user)) {
            user = connectionProperties.getProperty("username");
            if (StringUtils.isBlank(user)) {
                user = connectionProperties.getProperty("userName");
            }
            if (user != null)
                amendedProps.setProperty("user", user);
        }

        if (secret == null || connectionProperties.get("password") != null)
            return DriverManager.getConnection(url, amendedProps);

        amendedProps.put("password", secret.toString());

        logger.info("connecting as " + user + " to: " + url);

        return DriverManager.getConnection(url, amendedProps);
    }
}
