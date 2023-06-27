package org.sv.flexobject.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class HadoopTask extends Configured {
    static Logger logger = Logger.getLogger(HadoopTask.class);

    private static HadoopTask instance = null;

    private HadoopTaskConf taskConf = InstanceFactory.get(HadoopTaskConf.class);
    private String userName;
    private boolean configured = false;

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
            userName = conf.get(MRJobConfig.USER_NAME, System.getProperty("user.name"));
        }
    }

    public void setConf(SparkContext sc){
        super.setConf(sc.hadoopConfiguration());
        taskConf = new HadoopTaskConf(HadoopPropertiesWrapper.SPARK_NAMESPACE);
        taskConf.from(sc.getConf());
        userName = sc.sparkUser();
//    sparkConf.get("spark.job.user.name");
//        if (StringUtils.isBlank(userName))
//            logger.warn("Unknown job user name. Please set property 'spark.job.user.name'");
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
        return getInstance().getUserName();
    }

    public String getUserName(){
        return userName;
    }


    public static HadoopTaskConf getTaskConf(){
        return getInstance().taskConf;
    }

    public static void configure (SparkContext sc) throws Exception {
        // TODO: not sure if that makes any sense :
        //        Configuration hadoopConf = new Configuration();
//        for (Tuple2<String, String> t : conf.getAll()){
//            hadoopConf.set();
//        }
//        HadoopInstanceFactory.setConf(conf);
        getInstance().setConf(sc);
        getInstance().postConfigure();
    }

    public static void configure (Configuration conf) throws Exception {
        HadoopInstanceFactory.setConf(conf);
        getInstance().setConf(conf);
        getInstance().postConfigure();
    }

    protected void postConfigure() throws Exception {
        ConnectionManager.addProviders(getTaskConf().getConnectionManagerProviders());
        ConnectionManager.forEachProvider(Configurable.class, (p)->((Configurable)p).setConf(getConf()));

        ConnectionManager.getInstance().setDeploymentLevel(getTaskConf().getDeploymentLevel());
        ConnectionManager.getInstance().setEnvironment(getTaskConf().getConnectionManagerEnvironment());

        configured = true;
        logger.info("Hadoop Task is configured with " + getTaskConf().toString());
    }

    public static boolean isConfigured(){
        return getInstance().configured;
    }

    public static void clearConfiguration(){
        getInstance().configured = false;
    }

    public static class Builder{
        private ConnectionManager.DeploymentLevel deploymentLevel;
        private String connectionManagerEnvironment;
        private List<Class> connectionManagerProviders = new ArrayList<>();
        private List<ConnectionProvider> connectionProviders = new ArrayList<>();
        private List<URL> configurationResources = new ArrayList<>();

        protected Builder(){}

        public Builder deploymentLevel(ConnectionManager.DeploymentLevel deploymentLevel){
            this.deploymentLevel = deploymentLevel;
            return this;
        }

        public Builder environment(String connectionManagerEnvironment){
            this.connectionManagerEnvironment = connectionManagerEnvironment;
            return this;
        }

        public Builder addProvider(Class providerClass){
            this.connectionManagerProviders.add(providerClass);
            return this;
        }

        public Builder addProviders(Class ... providerClasses){
            this.connectionManagerProviders.addAll(Arrays.asList(providerClasses));
            return this;
        }

        public Builder addProviders(Collection<Class> providerClasses){
            this.connectionManagerProviders.addAll(providerClasses);
            return this;
        }

        public Builder addProvider(ConnectionProvider connectionProvider){
            this.connectionProviders.add(connectionProvider);
            return this;
        }

        public Builder addProviders(ConnectionProvider ... connectionProviders){
            this.connectionProviders.addAll(Arrays.asList(connectionProviders));
            return this;
        }

        public Builder addResource(URL resource){
            this.configurationResources.add(resource);
            return this;
        }

        public Builder addResources(URL ... resources){
            this.configurationResources.addAll(Arrays.asList(resources));
            return this;
        }

        public Builder addResource(String resource){
            addResource(getClass().getClassLoader().getResource(resource));
            return this;
        }

        public Builder addResources(String ... resources){
            ClassLoader classLoader = getClass().getClassLoader();
            for (String resource : resources)
                addResource(classLoader.getResource(resource));
            return this;
        }

        public Configuration buildConfiguration() throws Exception {
            HadoopTaskConf conf = InstanceFactory.get(HadoopTaskConf.class);
            if(deploymentLevel != null)
                conf.setDeploymentLevel(deploymentLevel);
            if(connectionManagerEnvironment != null)
                conf.setConnectionManagerEnvironment(connectionManagerEnvironment);
            if (!connectionManagerProviders.isEmpty())
                conf.setConnectionManagerProviders(connectionManagerProviders);

            Configuration rawConf = InstanceFactory.get(Configuration.class);
            for (URL resource: configurationResources)
                rawConf.addResource(resource);
            conf.update(rawConf);

            return rawConf;
        }

        public void build() throws Exception {
            Configuration rawConf = buildConfiguration();
            HadoopTask.configure(rawConf);

            for (ConnectionProvider provider : connectionProviders){
                if (provider instanceof Configurable)
                    ((Configurable) provider).setConf(rawConf);
                ConnectionManager.getInstance().registerProvider(provider);
            }
        }
    }

    public static Builder builder(){
        return new Builder();
    }
}
