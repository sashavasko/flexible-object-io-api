package org.sv.flexobject.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

public class RecordDetails<T> {
    String topic;
    T value;
    RecordMetadata metadata;

    public RecordDetails(String topic, T value) {
        this.value = value;
        this.topic = topic;
    }


    public RecordMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(RecordMetadata metadata) {
        this.metadata = metadata;
    }

    public T getValue() {
        return value;
    }

    public String getTopic() {
        return topic;
    }
}
