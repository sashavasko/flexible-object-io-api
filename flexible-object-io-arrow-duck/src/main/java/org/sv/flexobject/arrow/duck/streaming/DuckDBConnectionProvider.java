package org.sv.flexobject.arrow.duck.streaming;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.duckdb.DuckDBConnection;
import org.sv.flexobject.connections.ConnectionProvider;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class DuckDBConnectionProvider  implements ConnectionProvider {

    public static final String DUCKDB_DRIVER = "org.duckdb.DuckDBDriver";
    static Logger logger = LogManager.getLogger(DuckDBConnectionProvider.class);

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return List.of(DuckDBConnection.class);
    }

    public static AutoCloseable getConnection(String driver, String url, Properties props) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        logger.info("connecting to DuckDB: " + url);
        return DriverManager.getConnection(url, props);
    }

    public static DuckDBConnection getConnection(String dbpath) throws ClassNotFoundException, SQLException {
        String url = "jdbc:duckdb:" + dbpath;
        return (DuckDBConnection) getConnection(DUCKDB_DRIVER, url, new Properties());
    }

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {

        String driverClassName = connectionProperties.getProperty("driverClassName");
        if (StringUtils.isBlank(driverClassName))
            driverClassName = DUCKDB_DRIVER;

        Properties amendedProps = new Properties();
        for (var prop : connectionProperties.stringPropertyNames()){
            if (prop.startsWith("duckdb."))
                amendedProps.put(prop, connectionProperties.getProperty(prop));
        }

        String url = connectionProperties.getProperty("url");
        return getConnection(driverClassName, url, amendedProps);
    }}
