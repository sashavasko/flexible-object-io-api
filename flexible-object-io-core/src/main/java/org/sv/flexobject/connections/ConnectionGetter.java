package org.sv.flexobject.connections;

import java.io.IOException;
import java.util.Properties;

public class ConnectionGetter implements Runnable {
    ConnectionProvider connectionProvider;
    String connectionName;
    Properties connectionProperties = new Properties();
    Object secret;
    AutoCloseable connection;
    Exception exception;

    boolean cancelled = false;

    public ConnectionGetter() {
    }

    public ConnectionGetter(ConnectionProvider connectionProvider, String connectionName, Properties connectionProperties, Object secret) {
        this.connectionProvider = connectionProvider;
        this.connectionName = connectionName;
        this.connectionProperties = connectionProperties;
        this.secret = secret;
    }

    public AutoCloseable getConnection() {
        return connection;
    }

    @Override
    public void run() {
        try {
            connection = connectionProvider.getConnection(connectionName, connectionProperties, secret);
            if (cancelled) {
                connection.close();
                connection = null;
            }
        } catch (Exception e) {
            this.exception = e;
        }
    }

    public ConnectionGetter withSecret(Object secret) {
        this.secret = secret;
        return this;
    }

    public ConnectionGetter usingProvider(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
        return this;
    }

    public ConnectionGetter forName(String connectionName) {
        this.connectionName = connectionName;
        return this;
    }

    public ConnectionGetter withProperties(Properties connectionProperties) {
        if (connectionProperties != null)
            this.connectionProperties.putAll(connectionProperties);

        return this;
    }

    public ConnectionGetter overrideProperties(Properties overrides) {
        if(overrides != null)
            connectionProperties.putAll(overrides);

        return this;
    }

    public boolean hasProperties() {
        return !connectionProperties.isEmpty();
    }

    public boolean hasSecret() {
        return secret != null;
    }

    public AutoCloseable connect() throws IOException {
        Thread thread = new Thread(this, "GetConnection-" + connectionName);
        thread.start();
        try {
            thread.join(60*1000);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while getting the connection", e);
        }
        if (connection != null)
            return connection;

        cancelled = true;
        if (exception != null){
            throw new IOException("Failed to get connection " + connectionName, exception);
        }
        return null;
    }

    public Properties getProperties() {
        return connectionProperties;
    }

    public boolean hasProvider() {
        return connectionProvider != null;
    }

    public boolean missingRequiredProperties() {
        return connectionProvider.requiresProperties() && !hasProperties();
    }
}
