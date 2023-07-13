package org.sv.flexobject.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.schema.DataTypes;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class DockerClientProvider implements ConnectionProvider {
    public static DockerClient getDefault() throws Exception {
        return getDockerClient(null, null, null);
    }

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        return getDockerClient(name, connectionProperties, secret);
    }

    public static DockerClient getDockerClient(String name, Properties connectionProperties, Object secret) throws Exception {
        DefaultDockerClientConfig.Builder configBuilder = DefaultDockerClientConfig
                .createDefaultConfigBuilder();

        // TODO not sure if we want that
        if (StringUtils.isNotBlank(name))
            configBuilder.withDockerContext(name);
        if (connectionProperties != null && !connectionProperties.isEmpty())
                configBuilder.withProperties(connectionProperties);

        if (secret != null)
            configBuilder.withRegistryPassword(DataTypes.stringConverter(secret));

        DefaultDockerClientConfig config = configBuilder.build();
        DockerHttpClient dockerHttpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .maxConnections(100)
                .connectionTimeout(Duration.ofSeconds(30))
                .responseTimeout(Duration.ofSeconds(45))
                .build();

        return DockerClientImpl.getInstance(config, dockerHttpClient);
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(DockerClient.class);
    }

    @Override
    public boolean requiresProperties() {
        return false;
    }
}
