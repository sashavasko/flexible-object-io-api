package org.sv.flexobject.sql.dao;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.adapter.AdapterFactory;
import org.sv.flexobject.sql.SqlInputAdapter;
import org.sv.flexobject.sql.SqlOutAdapter;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DataSourceDao implements AutoCloseable{

    protected String connectionName;
    protected DataSource ds;
    protected Connection connection = null;

    protected AdapterFactory adapterFactory = new AdapterFactory() {
        @Override
        public InAdapter createInputAdapter(String id) {
            return new SqlInputAdapter();
        }

        @Override
        public OutAdapter createOutputAdapter(String id) {
            return new SqlOutAdapter();
        }
    };

    public DataSourceDao(String connectionName) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.connectionName = connectionName;
        ds = BatchEnvironment.getInstance().loadDataSource(connectionName);
    }

    public void setAdapterFactory(AdapterFactory adapterFactory) {
        this.adapterFactory = adapterFactory;
    }

    public InAdapter createInputAdapter(String id){
        return adapterFactory.createInputAdapter(id);
    }

    public OutAdapter createOutputAdapter(String id){
        return adapterFactory.createOutputAdapter(id);
    }

    protected Connection getConnectionFromDataSource() throws SQLException {
        return ds.getConnection();
    }

    public Connection getConnection() throws SQLException {
        setupConnection();
        return connection;
    }

    public void setupConnection() throws SQLException {
        if (connection == null || connection.isClosed()){
            connection = getConnectionFromDataSource();
        }
    }

    public void invalidateConnection(){
        try {
            closeConnection();
        } catch (SQLException e) {
        }
        connection = null;
    }

    public void closeConnection() throws SQLException {
        if (connection != null && !connection.isClosed())
            connection.close();

        connection = null;
    }

    @Override
    public void close() throws Exception {
        closeConnection();
    }
}
