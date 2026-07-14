package org.sv.flexobject.kafka.streaming;

import com.fasterxml.jackson.databind.jsonschema.JsonSchema;
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchema;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.avro.AvroSerializationStrategy;
import org.sv.flexobject.avro.AvroSerializer;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.serde.JsonSerializationStrategy;
import org.sv.flexobject.serde.SerializationStrategy;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.AutoCloseables;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KafkaSource<T extends Streamable> implements Source<T>, Iterator<T>, AutoCloseable {

    public static final Logger logger = LogManager.getLogger(KafkaSource.class);

    KafkaConsumer<byte[], byte[]> kafkaConsumer;
    Duration timeout = Duration.ofSeconds(60);
    SerializationStrategy serde = AvroSerializationStrategy.AVRO;
    ConsumerRecords<byte[],byte[]> currentRecords = null;
    Iterator<ConsumerRecord<byte[],byte[]>> currentRecordIterator = null;
    Class <? extends Streamable> schema;
    SchemaRegistryClient schemaRegistryClient;

    protected KafkaSource(){}

    public static class Builder<ST extends Streamable>{
        KafkaSource<ST> source = new KafkaSource<>();
        Set<String> topics = new HashSet<>();
        Integer partition;
        boolean startAtTheBeginning = false;
        Long startOffset;
        String connectionName;
        String groupId;


        public Builder<ST> addTopic(String topic){
            topics.add(topic);
            return this;
        }

        public Builder<ST> forPartition(int partition){
            this.partition = partition;
            return this;
        }

        public Builder<ST> startingAt(long position){
            this.startOffset = position;
            return this;
        }

        public Builder<ST> startingAtTheBeginning(){
            this.startAtTheBeginning = true;
            return this;
        }

        public Builder<ST> forSchema(Class<? extends Streamable> schema){
            source.schema = schema;
            return this;
        }

        public Builder<ST> withSchemaRegistry(SchemaRegistryClient schemaRegistryClient){
            source.schemaRegistryClient = schemaRegistryClient;
            return this;
        }

        public Builder<ST> deserializeWith(SerializationStrategy strategy){
            source.serde = strategy;
            return this;
        }

        public Builder<ST> forConnection(String name) {
            connectionName = name;
            return this;
        }

        public Builder<ST> groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder<ST> useConsumer(KafkaConsumer<byte[], byte[]> consumer){
            source.kafkaConsumer = consumer;
            return this;
        }
        public Builder<ST> timeoutSeconds(long seconds){
            source.timeout = Duration.ofSeconds(seconds);
            return this;
        }

        public Builder<ST> timeoutMillis(long millis){
            source.timeout = Duration.ofMillis(millis);
            return this;
        }

        public KafkaSource<ST> build() throws Exception {
            if (topics.isEmpty())
                throw new IllegalArgumentException("KafkaSource has to be subscribed to at least 1 topic");
            if (source.schema == null)
                logger.warn("Schema for Kafka Source is not set - will attempt to derive schema from the message");

            if (source.kafkaConsumer == null)
                getKafkaConsumer();

            if (partition == null) {
                source.kafkaConsumer.subscribe(topics);
            } else if (topics.size() != 1){
                throw new IllegalArgumentException("Kafka Source can handle exactly one partition of a single topic");
            } else {
                String topic = topics.iterator().next();
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                List<TopicPartition> partitions = List.of(topicPartition);
                source.kafkaConsumer.assign(partitions);
                if (startOffset != null) {
                    source.kafkaConsumer.seek(topicPartition, startOffset);
                } else if (startAtTheBeginning){
                    source.kafkaConsumer.seekToBeginning(partitions);
                } else
                    source.kafkaConsumer.seekToEnd(partitions);
            }

            return source;
        }

        private void getKafkaConsumer() {
            Properties overrides = new Properties();
            if (groupId != null)
                overrides.put("group.id", groupId);
            if (partition == null && startAtTheBeginning)
                overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");;

            try {
                @SuppressWarnings("unchecked")
                KafkaConsumer<byte[], byte[]> consumer = ConnectionManager.getConnection(KafkaConsumer.class, connectionName, overrides);
                source.kafkaConsumer = consumer;
            } catch (Exception e) {
                throw new IllegalArgumentException("Kafka connection failed (invalid connection name?)", e);
            }
        }
    }

    public static <ST extends Streamable> Builder<ST> builder(){
        return new Builder<>();
    }

    public KafkaConsumer<byte[], byte[]> getConsumer(){
        return kafkaConsumer;
    }

    protected boolean pollRecords(){
        if (kafkaConsumer == null)
            return false;

        if (currentRecords == null || !currentRecordIterator.hasNext()){
            currentRecords = kafkaConsumer.poll(timeout);
            currentRecordIterator = currentRecords.iterator();
            if (currentRecords.isEmpty())
                return false;
        }
        return currentRecordIterator.hasNext();
    }

    @Override
    public T get() throws Exception {
        return next();
    }


    @Override
    public boolean isEOF() {
        return !pollRecords();
    }

    @Override
    public void ack(){
        kafkaConsumer.commitSync(timeout);
    }

    @Override
    public void close() {
        kafkaConsumer.close();
        kafkaConsumer = null;
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public void setEOF() {
        AutoCloseables.closeQuietly(this);
    }

    @Override
    public boolean hasNext() {
        return pollRecords();
    }

    @Override
    public T next() {
        if (!pollRecords())
            return null;

        ConsumerRecord<byte[], byte[]> record = currentRecordIterator.next();
        Headers headers = record.headers();
        byte[] bytes = record.value();
        String topic = record.topic();

        T streamable = instantiate(topic, headers, bytes);
        if (streamable != null) {
            try {
                deserialize(streamable, headers, bytes);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize " + streamable.getClass().getName(), e);
            }
        }
        return streamable;
    }

    private T instantiate(String topic, Headers headers, byte[] bytes) {
        Class<? extends Streamable> clazz = schema;
        if (clazz == null) {
            Header header = headers.lastHeader(KafkaSink.SCHEMA_CLASS_HEADER);
            if (header != null){
                String className = new String(header.value(), StandardCharsets.UTF_8);
                try {
                    clazz = (Class<? extends Streamable>) DataTypes.classConverter(className);
                } catch (Exception e) {
                    logger.error("Failed to parse Class from schema name: " + className, e);
                }
            } else if (schemaRegistryClient != null) {
                ParsedSchema parsedSchema = null;
                if (bytes[0] == RegistryAwareAvroSerializationStrategy.MAGIC) {
                    int schemaId = DataTypes.bytesToInt(bytes, 1);
                    try {
                        parsedSchema = schemaRegistryClient.getSchemaById(schemaId);
                    } catch (Exception e) {
                        logger.error("Failed to obtain Schema from Registry for ID: " + schemaId, e);
                    }
                } else {
                    header = headers.lastHeader(KafkaSink.SCHEMA_GUID_HEADER);
                    if (header != null) {
                        String schemaGuid = new String(header.value(), StandardCharsets.UTF_8);
                        try {
                            parsedSchema = schemaRegistryClient.getSchemaByGuid(schemaGuid, "");
                        } catch (Exception e) {
                            logger.error("Failed to obtain Schema from Registry for GUID: " + schemaGuid, e);
                        }
                    } else {
                        String subject = RegistryAwareAvroSerializationStrategy.formatSubject(topic);
                        try {
                            SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
                            parsedSchema = schemaRegistryClient.getSchemaBySubjectAndId(subject, metadata.getId());
                        } catch (Exception ignored) {
                        }
                    }
                }
                if (parsedSchema != null) {
                    if (parsedSchema instanceof AvroSchema avroSchema) {
                        clazz = AvroSerializer.forSchema(avroSchema.rawSchema()).getDataClass();
                    } else if (parsedSchema instanceof ProtobufSchema) {
                        logger.error("Protobuf schemas are not supported. Please use Avro Schema or specify Java class extending Streamable.");
                    } else {
                        logger.error("Unsupported message schema type: " + parsedSchema.schemaType() + ". Please use Avro Schema or specify Java class extending Streamable.");
                    }
                }
            }
        }
        if (clazz != null){
            @SuppressWarnings("unchecked")
            T instance = (T) InstanceFactory.get(clazz);
            return instance;
        }
        return null;
    }

    protected void deserialize(T streamable, Headers headers, byte[] bytes) throws IOException {
        SerializationStrategy serde = this.serde;
        int dataOffset = 0;
        if (headers != null) {
            for (Header header : headers.headers(KafkaSink.CONTENT_TYPE_HEADER)) {
                if (Arrays.equals(header.value(), JsonSerializationStrategy.CONTENT_TYPE)) {
                    serde = JsonSerializationStrategy.JSON;
                } else if (Arrays.equals(header.value(), AvroSerializationStrategy.CONTENT_TYPE)) {
                    serde = AvroSerializationStrategy.AVRO;
                    if (bytes[0] == RegistryAwareAvroSerializationStrategy.MAGIC)
                        dataOffset = 5;
                }
            }
        }

        if (serde == null){
            if (bytes[0] == RegistryAwareAvroSerializationStrategy.MAGIC) {
                dataOffset = 5;
                serde = AvroSerializationStrategy.AVRO;
            } else {
                if (bytes[0] == '{' || Character.isWhitespace(bytes[0])) {
                    try {
                        MapperFactory.getObjectReader().readTree(bytes);
                        serde = JsonSerializationStrategy.JSON;
                    } catch(IOException unused){
                    }
                }
                if (serde == null)
                    serde = AvroSerializationStrategy.AVRO;
            }
        }
        serde.deserialize(streamable, bytes, dataOffset, bytes.length - dataOffset);
    }
}
