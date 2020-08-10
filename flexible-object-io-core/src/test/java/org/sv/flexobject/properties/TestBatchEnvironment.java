package org.sv.flexobject.properties;

import org.sv.flexobject.sql.dao.BasicBatchEnvironment;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class TestBatchEnvironment extends BasicBatchEnvironment {
    protected static Map<String, DataSource> dataSources = new HashMap<>();

    @Override
    public DataSource loadDataSource(String connectionName) {
        return dataSources.get(connectionName) == null ? super.loadDataSource(connectionName) : dataSources.get(connectionName);
    }

    public static void setDataSource(String connectionName, DataSource dataSource) {
        TestBatchEnvironment.dataSources.put(connectionName, dataSource);
    }

    public static void clearDataSources(){
        dataSources.clear();
    }

}
