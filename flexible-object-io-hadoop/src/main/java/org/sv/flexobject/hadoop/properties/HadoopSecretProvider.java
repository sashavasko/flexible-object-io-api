package org.sv.flexobject.hadoop.properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.SecretProvider;
import org.sv.flexobject.hadoop.utils.IConfigured;

import java.io.IOException;
import java.util.Properties;

public class HadoopSecretProvider implements SecretProvider, IConfigured {

    Logger logger = Logger.getLogger(HadoopSecretProvider.class);

    public String getNameNodeRPC(int order){
        String nameservice = getConf().get("dfs.nameservices");
        if (StringUtils.isNotBlank(nameservice)){
            String namenodes = getConf().get("dfs.ha.namenodes." + nameservice);
            String[] nodes = namenodes.split(",");
            if (order >= nodes.length)
                return null;
            String rpc = getConf().get("dfs.namenode.servicerpc-address." + nameservice + "." + nodes[order]);
            return rpc;
        }
        return null;
    }

    public String getActiveNameNodeRPC() {
        String activeNameNode;
        if (StringUtils.isBlank(getConf().get("dfs.nameservices"))){
            activeNameNode = getConf().get("dfs.namenode.servicerpc-address");
        } else {
            try {
                activeNameNode = getNameNodeRPC(0);
                FileSystem.get(new Path("hdfs://" + activeNameNode + "/").toUri(), getConf()).isFile(new Path("/tmp"));
            } catch (Exception e) {
                try {
                    activeNameNode = getNameNodeRPC(1);
                    FileSystem.get(new Path("hdfs://" + activeNameNode + "/").toUri(), getConf()).isFile(new Path("/tmp"));
                } catch (IOException ex) {
                    logger.error("Cannot find any active Name Node", ex);
                    throw new RuntimeException("Cannot determine active Name Node", ex);
                }
            }
        }
        logger.info("Active HDFS Name Node rpc is determined to be " + activeNameNode);
        return activeNameNode;
    }

    public String getUserName(){
        return getConf().get(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    }

    public String getPassword(String propertyName, ConnectionManager.DeploymentLevel deploymentLevel) {
        String credentialsPath = "jceks://hdfs@"
                + getActiveNameNodeRPC()
                + "/user/"
                + getUserName()
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
        return getPassword("cfx.hadoop.db." + connectionName + ".password", deploymentLevel);
    }

    @Override
    public Properties getProperties(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) {
        return null;
    }
}
