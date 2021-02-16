package org.sv.flexobject.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PreparedRecord<K,T> {
    ProducerRecord<K,T> record;
    Callback callback;

    public PreparedRecord(String topic, K key, T value, Callback callback) {
        this.record = new ProducerRecord<K,T>(topic, key, value);
        this.callback = callback;
    }

    public ProducerRecord<K, T> getRecord() {
        return record;
    }

    public Callback getCallback() {
        return callback;
    }
}
