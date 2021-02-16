package org.sv.flexobject.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;

public abstract class DataTranslator<T> implements AutoCloseable {

    T data;

    public DataTranslator(AbstractConfig config) {
    }

    public T getData() {
        return data;
    }

    public DataTranslator setData(T data) {
        this.data = data;
        return this;
    }

    abstract public DataTranslator setTimestamp(long timestamp);

    public Schema getKeySchema() {
        return Schema.BYTES_SCHEMA;
    }

    public Schema getValueSchema() {
        return Schema.STRING_SCHEMA;
    }

    abstract public Object getKey();

    abstract public Object getValue();
}
