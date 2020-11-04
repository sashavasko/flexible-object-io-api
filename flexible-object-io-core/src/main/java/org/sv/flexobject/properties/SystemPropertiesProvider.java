package org.sv.flexobject.properties;

import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.PropertiesProvider;

import java.util.Map;
import java.util.Properties;

public class SystemPropertiesProvider implements PropertiesProvider {

    Namespace namespace = Namespace.DB;

    public SystemPropertiesProvider() {
    }

    public SystemPropertiesProvider(Namespace namespace) {
        this.namespace = namespace;
    }

    @Override
    public Properties getProperties(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) {
        Properties connectionProperties = new Properties();
        String connectionNamespace = namespace.getSettingName(connectionName);
        for (Map.Entry prop : System.getProperties().entrySet()){
            String key = (String) prop.getKey();
            if (key.startsWith(connectionNamespace))
                connectionProperties.put(key.substring(connectionNamespace.length() + 1), prop.getValue());
        }
        return connectionProperties;
    }
}
