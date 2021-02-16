package org.sv.flexobject.kafka.connect;

import org.apache.kafka.common.config.ConfigException;

public class ConnectConfigException extends ConfigException {
    private final String name;
    private final Object value;

    public ConnectConfigException(final String name, final Object value, final String message) {
        super(name, value, message);
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

}
