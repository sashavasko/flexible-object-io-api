package org.sv.flexobject.hadoop.properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.SecretProvider;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.hadoop.utils.IConfigured;

import java.io.IOException;
import java.util.Properties;

public class HadoopSecretProvider implements SecretProvider, IConfigured {

    Logger logger = Logger.getLogger(HadoopSecretProvider.class);

    public String getPassword(String propertyName, ConnectionManager.DeploymentLevel deploymentLevel) {
        String credentialsPath = "jceks://hdfs@"
                + HadoopTask.getActiveNameNodeRPC(getConf())
                + "/user/"
                + HadoopTask.getUserName(getConf())
                + "/creds/"
                + deploymentLevel;
        logger.info("Using credentials path : " + credentialsPath);
        getConf().set("hadoop.security.credential.provider.path", credentialsPath + "/secret.jceks");

        try {
            char[] passwordBytes = getConf().getPassword(propertyName);
            if (passwordBytes != null)
                return new String(passwordBytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load password property " + propertyName, e);
        }
        logger.error("Cannot find password property " + propertyName);
        return null;
    }


    @Override
    public Object getSecret(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) {
        return getPassword(HadoopTask.getTaskConf().getNamespace() + ".db." + connectionName + ".password", deploymentLevel);
    }

    @Override
    public Properties getProperties(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) {
        return null;
    }
}
