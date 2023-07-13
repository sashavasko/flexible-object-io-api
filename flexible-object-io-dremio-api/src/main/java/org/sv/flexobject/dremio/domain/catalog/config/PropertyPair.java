package org.sv.flexobject.dremio.domain.catalog.config;


import org.sv.flexobject.StreamableImpl;

public class PropertyPair extends StreamableImpl {
    String name;
    String value;

    public PropertyPair() {
    }

    public PropertyPair(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
}
