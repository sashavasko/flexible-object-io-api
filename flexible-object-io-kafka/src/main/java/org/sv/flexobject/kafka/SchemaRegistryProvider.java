package org.sv.flexobject.kafka;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.sv.flexobject.connections.ConnectionProvider;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SchemaRegistryProvider implements ConnectionProvider {
    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        String url = connectionProperties.getProperty("url", connectionProperties.getProperty("schema.registry.url"));
        @SuppressWarnings({"unchecked", "rawTypes"})
        SchemaRegistryClient client = new CachedSchemaRegistryClient(url, 1000, (Map)connectionProperties);
        return client;
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return List.of(SchemaRegistryClient.class);
    }
}
