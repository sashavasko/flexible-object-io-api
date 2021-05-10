package org.sv.flexobject.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesProvider;
import org.sv.flexobject.hadoop.properties.HadoopSecretProvider;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;
import org.sv.flexobject.sql.providers.UnPooledConnectionProvider;

import java.io.IOException;

public class HadoopTask extends Configured {
    static Logger logger = Logger.getLogger(HadoopTask.class);
    public static final String DEFAULT_NAMESPACE = "org.sv.flexobject";

    private static HadoopTask instance = null;

    private HadoopTaskConf taskConf = new HadoopTaskConf();

    private HadoopTask(){}

    public static HadoopTask getInstance(){
        if (instance == null)
            instance = new HadoopTask();
        return instance;
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf != null){
            taskConf.from(conf);
        }
    }

    public String getNameNodeRPC(int order){
        return getNameNodeRPC(getConf(), order);
    }

    public static String getNameNodeRPC(Configuration conf, int order){
        String nameservice = conf.get("dfs.nameservices");
        if (StringUtils.isNotBlank(nameservice)){
            String namenodes = conf.get("dfs.ha.namenodes." + nameservice);
            String[] nodes = namenodes.split(",");
            if (order >= nodes.length)
                return null;
            String rpc = conf.get("dfs.namenode.servicerpc-address." + nameservice + "." + nodes[order]);
            return rpc;
        }
        return null;
    }

    public String getActiveNameNodeRPC() {
        return getActiveNameNodeRPC(getConf());
    }

    public static String getActiveNameNodeRPC(Configuration conf) {
        String activeNameNode;
        if (StringUtils.isBlank(conf.get("dfs.nameservices"))){
            activeNameNode = conf.get("dfs.namenode.servicerpc-address");
            if (activeNameNode == null)
                activeNameNode = conf.get("dfs.namenode.rpc-address");
        } else {
            try {
                activeNameNode = getNameNodeRPC(conf, 0);
                FileSystem.get(new Path("hdfs://" + activeNameNode + "/").toUri(), conf).getFileStatus(new Path("/tmp")).isFile();
            } catch (Exception e) {
                try {
                    activeNameNode = getNameNodeRPC(conf, 1);
                    FileSystem.get(new Path("hdfs://" + activeNameNode + "/").toUri(), conf).getFileStatus(new Path("/tmp")).isFile();
                } catch (IOException ex) {
                    logger.error("Cannot find any active Name Node", ex);
                    throw new RuntimeException("Cannot determine active Name Node", ex);
                }
            }
        }
        logger.info("Active HDFS Name Node rpc is determined to be " + activeNameNode);
        return activeNameNode;
    }

    public static String getUserName(Configuration conf){
        return conf.get(MRJobConfig.USER_NAME, System.getProperty("user.name"));
    }

    public String getUserName(){
        return getUserName(getConf());
    }

    public static HadoopTaskConf getTaskConf(){
        return getInstance().taskConf;
    }

    public static void configure (Configuration conf) throws Exception {
        HadoopInstanceFactory.setConf(conf);
        getInstance().setConf(conf);

        ConnectionManager.addProviders(getTaskConf().getConnectionManagerProviders());
        ConnectionManager.forEachProvider(Configurable.class, (p)->((Configurable)p).setConf(conf));

        ConnectionManager.getInstance().setDeploymentLevel(getTaskConf().getDeploymentLevel());
        ConnectionManager.getInstance().setEnvironment(getTaskConf().getConnectionManagerEnvironment());
    }
}
