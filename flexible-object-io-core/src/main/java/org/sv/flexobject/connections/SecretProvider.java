package org.sv.flexobject.connections;

import java.util.Properties;

public interface SecretProvider extends PropertiesProvider{

    default Object getSecret(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment, Properties properties){
        return getSecret(connectionName, deploymentLevel, environment);
    }

    default Object getSecret(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment){
        Properties properties = getProperties(connectionName, deploymentLevel, environment);
        return properties.get("password");
    }

}
