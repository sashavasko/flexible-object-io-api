package org.sv.flexobject.rabbit;

import com.rabbitmq.client.Address;
import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.connections.ConnectionConf;
import org.sv.flexobject.util.InstanceFactory;

import java.net.URI;
import java.util.concurrent.ExecutorService;

public class RabbitConnectionConf extends ConnectionConf<RabbitConnectionConf> {

    String host;
    String virtualHost;
    String uri;
    String username;
    String password;
    int port;
    String addresses;
    String clientProviderName;
    boolean automaticRecoveryEnabled;
    long recoveryIntervalMillis;

    public RabbitConnectionConf() {
    }

    public RabbitConnectionConf(String host, int port, String username) {
        this.host = host;
        this.port = port;
        this.username = username;
    }

    public String getClientProviderName() {
        return clientProviderName;
    }

    Class<? extends ExecutorService> executorService;

    @Override
    public RabbitConnectionConf setDefaults() {
        port = 5672;
        automaticRecoveryEnabled = true;
        recoveryIntervalMillis = 6000;
        return this;
    }

    public String getHost() {
        return host;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getPort() {
        return port;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public URI getUri() {
        return uri == null ? null : URI.create(uri);
    }

    public boolean hasAddresses() {
        return (StringUtils.isNotBlank(addresses) || StringUtils.isNotBlank(host));
    }
    public Address[] getAddresses() {
        if (StringUtils.isNotBlank(addresses)){
            return Address.parseAddresses(addresses);
        } else if (StringUtils.isNotBlank(host)){
            return new Address[]{new Address(host, port)};
        }
        return null;
    }

    public Class<? extends ExecutorService> getExecutorServiceClass() {
        return executorService;
    }

    public boolean hasExecutorService() {
        return executorService != null;
    }

    public ExecutorService getExecutorService() {
        return InstanceFactory.get(executorService);
    }

    public boolean hasURI() {
        return uri != null;
    }

    public boolean isAutomaticRecovery() {
        return automaticRecoveryEnabled;
    }

    public long getRecoveryIntervalMillis() {
        return recoveryIntervalMillis == 0 ? 6000 : recoveryIntervalMillis;
    }
}
