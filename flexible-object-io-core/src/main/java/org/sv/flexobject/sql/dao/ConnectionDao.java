package org.sv.flexobject.sql.dao;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.adapter.AdapterFactory;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.sql.SqlInputAdapter;
import org.sv.flexobject.sql.SqlOutAdapter;
import org.sv.flexobject.sql.connection.ConnectionWrapper;
import org.sv.flexobject.util.InstanceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionDao extends ConnectionWrapper implements AutoCloseable{

    protected String connectionName;

    protected AdapterFactory adapterFactory = new AdapterFactory() {
        @Override
        public InAdapter createInputAdapter(String id) {
            return InstanceFactory.get(SqlInputAdapter.class);
        }

        @Override
        public OutAdapter createOutputAdapter(String id) {
            return InstanceFactory.get(SqlOutAdapter.class);
        }
    };

    public ConnectionDao(){}

    public ConnectionDao(String connectionName) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.connectionName = connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
        invalidateConnection();
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

    public Connection setupConnection() throws SQLException {
        try {
            return (Connection) ConnectionManager.getInstance().getConnection(Connection.class, connectionName);
        } catch (Exception e) {
            if (e instanceof SQLException)
                throw (SQLException) e;
            throw new SQLException("Failed nto get connection from ConnectionManager", e);
        }
    }
}
