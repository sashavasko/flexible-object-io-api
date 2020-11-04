package org.sv.flexobject.sql.dao;

import org.apache.commons.dbcp2.BasicDataSource;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.ConnectionProvider;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BasicDataSourceProvider implements ConnectionProvider, AutoCloseable {

    private Map<String, BasicDataSource> dataSources = new HashMap<>();

    public static void register(){
        ConnectionManager.getInstance().registerProvider(new BasicDataSourceProvider());
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
            dataSource.setUrl(connectionProperties.getProperty("url"));
            dataSource.setUsername(connectionProperties.getProperty("username"));
            if (secret != null)
                dataSource.setPassword((String) secret);
            dataSources.put(name, dataSource);
        }
        return dataSource.getConnection();
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(Connection.class);
    }


    public void closeConnection(String name) throws Exception{
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
