package org.sv.flexobject.hadoop;

import org.apache.spark.sql.SparkSession;

public class SparkMain {

    /**
     * The entry point into Spark application. This will be run by Application Master.
     * By default it will instantiate Spark session based on spark.* options supplied in
     * configuration file via -conf command line option. This configuration is bundled
     * with jar file and uploaded to the cluster by Spark Client when run via hdpspark command.
     *<p>
     * main() method will attempt to instantiate user defined class:
     *
     *<pre>{@code
     * public class MySparkRunner extends SparkTool{
     *      int run(String[] var1) throws Exception{
     *
     *          // your workflow goes here
     *
     *          return 0;
     *      }
     * }
     *}</pre>
     *
     * that class should be specified in spark configuration like so:
     *<pre>{@code
     *     -Dspark.hadoop.tool.class=my.package.MySparkRunner
     *}</pre>
     *<p>
     * or via config file like so :
     *<pre>{@code
     *      <property><name>spark.hadoop.tool.class</name><value>my.package.MySparkRunner</value></property>
     *}</pre>
     *<p>
     * For unit testing the following steps should be followed :
     *<p>
     * Setup unit tests by creating local SparkSession like so :
     * (optionally adding some of the desired configuration parameters)
     *<p>
     *<pre>{@code
     *   SparkSession spark = SparkSession.builder()
     *      .master("local")
     *      .config("option", "value")
     *      .getOrCreate()
     *}</pre>
     *<p>
     * If you need to work with Connection Providers - configure the HadoopTask like so :
     * <pre>{@code
     *      HadoopTask.configure(spark.sparkContext());
     *}</pre>
     *<p>
     * If you are testing the SparkTool implementation - make sure you supply spark session to it like so :
     *<pre>{@code
     *     tool.setSession(spark);
     *}</pre>
     *
     */

    public static void main(String[] args){
        SparkSession spark = SparkSession.builder()
                .getOrCreate();

        try {
            HadoopTask.configure(spark.sparkContext());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        SparkTool tool = HadoopTask.getTaskConf().getTool();
        if (tool == null)
            throw new RuntimeException("Must specify SparkTool implementation using spark.hadoop.tool.class property");

        tool.setSession(spark);

        try {
            tool.run(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(2);
        }
    }
}
