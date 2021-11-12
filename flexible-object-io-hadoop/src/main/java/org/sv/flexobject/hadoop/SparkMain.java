package org.sv.flexobject.hadoop;

import org.apache.spark.sql.SparkSession;

public class SparkMain {

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
