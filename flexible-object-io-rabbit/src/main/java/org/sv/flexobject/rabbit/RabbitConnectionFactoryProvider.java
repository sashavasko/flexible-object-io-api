package org.sv.flexobject.rabbit;

import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Arrays;
import java.util.Properties;

public class RabbitConnectionFactoryProvider implements ConnectionProvider {


    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        RabbitConnectionFactory rabbitConnectionFactory = InstanceFactory.get(RabbitConnectionFactory.class);
        rabbitConnectionFactory.applyProperties(connectionProperties);
        rabbitConnectionFactory.setPassword(secret);
        return rabbitConnectionFactory;
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(RabbitConnectionFactory.class);
    }
}
