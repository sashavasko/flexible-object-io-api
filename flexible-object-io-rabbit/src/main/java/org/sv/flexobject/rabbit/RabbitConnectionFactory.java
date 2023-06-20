package org.sv.flexobject.rabbit;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class RabbitConnectionFactory implements AutoCloseable{
    public ConnectionFactory get() {
        return connectionFactory;
    }

    ConnectionFactory connectionFactory = InstanceFactory.get(ConnectionFactory.class);

    @Override
    public void close() throws Exception {
    }

    protected void applyProperties(Properties connectionProperties) throws Exception {
        RabbitConnectionConf conf = InstanceFactory.get(RabbitConnectionConf.class);
        conf.from(connectionProperties);
        connectionFactory.setUsername(conf.getUsername());
        connectionFactory.setHost(conf.getHost());
        connectionFactory.setPort(conf.getPort());
        if (conf.hasURI()){
            connectionFactory.setUri(conf.getUri());
        }

        setPassword(conf.getPassword());
    }

    protected void setPassword(Object password){
        if (password != null)
            connectionFactory.setPassword((String)password);
    }

    public Connection getConnection() throws IOException, TimeoutException {
        return connectionFactory.newConnection();
    }
    public Connection getConnection(Address[] addresses) throws IOException, TimeoutException {
        return connectionFactory.newConnection(addresses);
    }


}
