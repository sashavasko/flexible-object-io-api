package org.sv.flexobject.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.spark.sql.SparkSession;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public abstract class SparkTool<SELF extends SparkTool> extends Configured implements Tool {

    SparkSession spark;
    HadoopPropertiesWrapper conf;

    public SparkTool() {
    }

    public void makeConf(){
        conf = HadoopTask.getTaskConf().instantiateConf();
        if (conf != null)
            conf.setNamespace(HadoopPropertiesWrapper.SPARK_NAMESPACE);
    }

    public SELF setSession(SparkSession sparkSession){
        spark = sparkSession;
        setConf(sparkSession.sparkContext().hadoopConfiguration());
        makeConf();
        if (conf != null)
            conf.from(sparkSession.sparkContext().getConf());

        return (SELF)this;
    }

    public SparkSession getSpark() {
        return spark;
    }

    public <T extends HadoopPropertiesWrapper> T getToolConf() {
        return (T)conf.getClass().cast(conf);
    }
}
