package org.sv.flexobject.hadoop;

import com.carfax.hadoop.properties.HadoopPropertiesWrapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.spark.sql.SparkSession;

public abstract class SparkTool<SELF extends SparkTool> extends Configured implements Tool {

    SparkSession spark;
    HadoopPropertiesWrapper conf;

    public SparkTool() {
    }

    public void makeConf(){
        conf = HadoopTask.getTaskConf().instantiateConfig();
        conf.setNamespace(HadoopPropertiesWrapper.SPARK_NAMESPACE);
    }

    public SELF setSession(SparkSession sparkSession){
        spark = sparkSession;
        setConf(sparkSession.sparkContext().hadoopConfiguration());
        makeConf();
        conf.from(sparkSession.sparkContext().getConf());

        return (SELF)this;
    }

    public SparkSession getSpark() {
        return spark;
    }
}
