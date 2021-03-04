package org.sv.flexobject.sql.providers;

import org.apache.commons.dbcp2.BasicDataSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BasicDataSourceProvider implements SqlConnectionProvider, AutoCloseable {

    private Map<String, BasicDataSource> dataSources = new HashMap<>();

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
