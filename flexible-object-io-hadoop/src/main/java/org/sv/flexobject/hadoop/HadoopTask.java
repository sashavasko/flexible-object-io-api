package org.sv.flexobject.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesProvider;
import org.sv.flexobject.hadoop.properties.HadoopSecretProvider;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;
import org.sv.flexobject.sql.providers.UnPooledConnectionProvider;

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
