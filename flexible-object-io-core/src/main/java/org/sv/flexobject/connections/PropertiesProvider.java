package org.sv.flexobject.connections;

import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.properties.Namespace;

import java.util.Map;
import java.util.Properties;

public interface PropertiesProvider {

    static  Properties fixUrl(Properties props){
        return fixUrl(props, "url");
    }

    static  Properties fixUrl(Properties props, String urlPropertyName){
        String urlString = props.getProperty(urlPropertyName);
        props.setProperty(urlPropertyName, urlString.replaceAll("&amp;", "&"));
        return props;
    }


    default Properties filter(Namespace namespace, Properties properties){
        Properties connectionProperties = filter(properties, namespace.getNamespace() + ".");
        return connectionProperties.isEmpty() ? filter(properties, null) : connectionProperties;
    }

    default Properties filter(Properties properties, String prefix){
        Properties filtered = new Properties();
        for (Map.Entry prop : properties.entrySet()) {
            String key = (String) prop.getKey();
            if (StringUtils.isNotBlank(prefix)) {
                if (key.startsWith(prefix))
                    filtered.put(key.substring(prefix.length()), prop.getValue());
            } else {
                if (!key.contains("."))
                    filtered.put(key, prop.getValue());
            }
        }
        return filtered;
    }



    Properties getProperties(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment);

}
