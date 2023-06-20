package org.sv.flexobject.rabbit;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RabbitConnectionProvider implements ConnectionProvider {
    private static final Map<String,ConnectionFactory> connectionFactories = new HashMap<>();

    public Connection getConnection(String name, RabbitConnectionConf conf, Object secret) throws Exception {
        ConnectionFactory connectionFactory = null;
        if (!connectionFactories.containsKey(name)) {
            synchronized (connectionFactories) {
                if (!connectionFactories.containsKey(name)) {
                    connectionFactory = InstanceFactory.get(ConnectionFactory.class);
                    connectionFactory.setUsername(conf.getUsername());
                    connectionFactory.setPassword((String) secret);
                    if (conf.hasURI()) {
                        connectionFactory.setUri(conf.getUri());
                    }

                    connectionFactory.setAutomaticRecoveryEnabled(conf.isAutomaticRecovery());
                    connectionFactory.setNetworkRecoveryInterval(conf.getRecoveryIntervalMillis());
                    if (conf.isAutomaticRecovery()) {
                        connectionFactories.put(name, connectionFactory);
                    }
                }
            }
        }
        if (connectionFactory == null)
            connectionFactory = connectionFactories.get(name);

        if (connectionFactory == null)
            throw new RabbitException("Failed to create Rabbit connection Factory");

        try {
            if (conf.hasAddresses()) {
                if (conf.hasExecutorService()) {
                    return connectionFactory.newConnection(conf.getExecutorService(), conf.getAddresses(), conf.getClientProviderName());
                } else
                    return connectionFactory.newConnection(conf.getAddresses(), conf.getClientProviderName());
            }
            return connectionFactory.newConnection(conf.getClientProviderName());
        }catch (Exception e){
            throw new RabbitException("Failed to connect to Rabbit using configuration " + conf, e);
        }
    }
    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        RabbitConnectionConf conf = InstanceFactory.get(RabbitConnectionConf.class);
        conf.from(connectionProperties);
        return getConnection(name, conf, secret);
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(com.rabbitmq.client.Connection.class);
    }
}
