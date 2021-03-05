package org.sv.flexobject.sql.providers;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionProvider;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BasicDataSourceProvider implements ConnectionProvider, AutoCloseable {
    static Logger logger = LogManager.getLogger(BasicDataSourceProvider.class);

    private Map<String, BasicDataSource> dataSources = new HashMap<>();

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(Connection.class, PooledSqlConnection.class);
    }

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        BasicDataSource dataSource = dataSources.get(name);
        if (dataSource == null) {
            String driverClassName = connectionProperties.getProperty("driverClassName");
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("JDBC Driver " + driverClassName + " is not available.");
            }

            dataSource = new BasicDataSource();
            dataSource.setDriverClassName(driverClassName);
            String url = connectionProperties.getProperty("url");
            String username = connectionProperties.getProperty("username");
            logger.info("connecting as " + username + " to: " + url);
            dataSource.setUrl(url);
            dataSource.setUsername(username);
            if (secret != null)
                dataSource.setPassword((String) secret);
            dataSources.put(name, dataSource);
        }
        return dataSource.getConnection();
    }

    synchronized public void closeConnection(String name) throws Exception{
        BasicDataSource ds = dataSources.get(name);
        if (ds != null) {
            ds.close();
            dataSources.remove(name);
        }
    }

    @Override
    public void close() throws Exception {
        Object[] keys = dataSources.keySet().toArray();
        for(Object key : keys)
            closeConnection((String) key);
    }
}
