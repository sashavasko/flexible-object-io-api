package org.sv.flexobject.properties;

import java.util.Map;

public interface Configurable {

    void configure(Map props) throws Exception;
    PropertiesWrapper getConfiguration();
}
