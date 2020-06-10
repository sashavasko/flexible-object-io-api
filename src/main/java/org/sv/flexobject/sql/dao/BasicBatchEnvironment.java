package org.sv.flexobject.sql.dao;

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BasicBatchEnvironment extends BatchEnvironment {

    @Override
    protected void logProgress(String message) {
        System.out.println(message);
    }

    @Override
    protected DataSource createDataSource(Properties props) {
        String driverClassName = props.getProperty("driverClassName");
        try {
            Class.forName(driverClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC Driver " + driverClassName + " is not available.");
        }
        BasicDataSource dataSource =  new BasicDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(props.getProperty("url"));
        dataSource.setUsername(props.getProperty("username"));
        dataSource.setPassword(props.getProperty("password"));
        return dataSource;
    }

    @Override
    protected List<File> getBaseDirs() {
        if (getClass().getSimpleName().contains("Test"))
            return Arrays.asList(new File("src/test/resources"));
        return Arrays.asList(new File("/etc"));
    }
}
