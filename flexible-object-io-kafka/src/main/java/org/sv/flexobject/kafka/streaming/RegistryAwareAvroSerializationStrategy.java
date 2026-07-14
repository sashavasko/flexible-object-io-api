package org.sv.flexobject.kafka.streaming;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.avro.AvroSerializationStrategy;
import org.sv.flexobject.avro.AvroSerializer;
import org.sv.flexobject.connections.ConnectionManager;

import java.io.IOException;

public class RegistryAwareAvroSerializationStrategy extends AvroSerializationStrategy {

    public static final byte MAGIC = 0x0;

    Class<? extends Streamable> schema;
    Integer schemaId;

    public static String formatSubject(String topic){
        return topic + "-value";
    }

    public static Integer getRegisteredSchemaId(Class<? extends Streamable> schema, SchemaRegistryClient schemaRegistryClient, boolean autoRegister, String topic) throws RestClientException, IOException {
        ParsedSchema parsedSchema = new AvroSchema(org.sv.flexobject.avro.AvroSchema.forClass(schema));
        String subject = formatSubject(topic);
        if (autoRegister){
            return schemaRegistryClient.register(subject, parsedSchema);
        } else if (schemaRegistryClient != null){
            return schemaRegistryClient.getId(subject, parsedSchema);
        }
        return null;
    }

    public static String getRegisteredSchemaGuid(Class<? extends Streamable> schema, SchemaRegistryClient schemaRegistryClient, boolean autoRegister, String topic) throws RestClientException, IOException {
        ParsedSchema parsedSchema = new AvroSchema(org.sv.flexobject.avro.AvroSchema.forClass(schema));
        String subject = formatSubject(topic);
        if (autoRegister){
            RegisterSchemaResponse response = schemaRegistryClient.registerWithResponse(subject, parsedSchema, false);
            return response.getGuid();
        } else if (schemaRegistryClient != null){
            return schemaRegistryClient.getGuid(subject, parsedSchema);
        }
        return null;
    }

    public static class Builder{
        RegistryAwareAvroSerializationStrategy strategy = new RegistryAwareAvroSerializationStrategy();
        SchemaRegistryClient schemaRegistryClient;
        boolean autoRegister = false;
        String topic;

        public Builder forSchema(Class<? extends Streamable> schema){
            strategy.schema = schema;
            return this;
        }

        public Builder autoRegister(SchemaRegistryClient client, String topicName){
            this.schemaRegistryClient = client;
            this.autoRegister = true;
            this.topic = topicName;
            return this;
        }

        public Builder autoRegister(String connectionName, String topicName) throws Exception {
            this.schemaRegistryClient = ConnectionManager.getConnection(SchemaRegistryClient.class, connectionName);
            this.autoRegister = true;
            this.topic = topicName;
            return this;
        }

        public RegistryAwareAvroSerializationStrategy build() throws RestClientException, IOException {
            if (strategy.schema == null)
                throw new IllegalArgumentException("Schema class extending Streamable must be specified for Schema Registry aware Serialization strategy.");
            strategy.schemaId = getRegisteredSchemaId(strategy.schema, schemaRegistryClient, autoRegister, topic);
            return strategy;
        }
    }

    public static Builder builder(){
        return new Builder();
    }

    @Override
    public byte[] serialize(Streamable datum) throws IOException {
        if (schemaId == null) {
            return super.serialize(datum);
        } else {
            return AvroSerializer.forData(datum).start().write(MAGIC).write(schemaId).write(datum).asBytes();
        }
    }
}
