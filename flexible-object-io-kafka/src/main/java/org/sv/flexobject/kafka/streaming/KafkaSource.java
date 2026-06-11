package org.sv.flexobject.kafka.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.AutoCloseables;
import org.sv.flexobject.util.InstanceFactory;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KafkaSource<T extends Streamable> implements Source<T>, Iterator<T>, AutoCloseable {
    KafkaConsumer<byte[], byte[]> kafkaConsumer;
    Duration timeout = Duration.ofSeconds(60);
    ConsumerRecords<byte[],byte[]> currentRecords = null;
    Iterator<ConsumerRecord<byte[],byte[]>> currentRecordIterator = null;
    Class <? extends Streamable> schema;

    protected KafkaSource(){}

    public static class Builder<ST extends Streamable>{
        KafkaSource<ST> source = new KafkaSource<>();
        Set<String> topics = new HashSet<>();

        public Builder<ST> addTopic(String topic){
            topics.add(topic);
            return this;
        }

        public Builder<ST> forSchema(Class<? extends Streamable> schema){
            source.schema = schema;
            return this;
        }

        public Builder<ST> forConnection(String name) throws Exception {
            @SuppressWarnings("unchecked")
            KafkaConsumer<byte[], byte[]> consumer = ConnectionManager.getConnection(KafkaConsumer.class, name);
            source.kafkaConsumer = consumer;
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

        public KafkaSource build(){
            if (topics.isEmpty())
                throw new IllegalArgumentException("KafkaSource has to be subscribed to at least 1 topic");
            if (source.kafkaConsumer == null)
                throw new IllegalArgumentException("Must specify a valid connection name for Kafka");
            if (source.schema == null)
                throw new IllegalArgumentException("Must specify a Streamable class as a schema for Kafka Source");
            source.kafkaConsumer.subscribe(topics);
            return source;
        }
    }

    public static <ST extends Streamable> Builder<ST> builder(){
        return new Builder<>();
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
        kafkaConsumer.commitSync();
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
        byte[] bytes = currentRecordIterator.next().value();

        @SuppressWarnings("unchecked")
        T streamable = (T) InstanceFactory.get(schema);
        try {
            streamable.fromJsonBytes(bytes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize " + schema.getName() + " from JSON bytes.", e);
        }
        return streamable;
    }
}
