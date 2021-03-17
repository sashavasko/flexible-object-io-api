package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.HadoopTask;

import java.io.IOException;
import java.util.Arrays;

abstract public class MapReduceDriver<SELF extends MapReduceDriver> extends ConfiguredDriver<SELF> implements IMapperHelper, IReducerHelper {

    Logger logger = Logger.getLogger(MapReduceDriver.class);

    Class<? extends InputFormat> inputFormatClass = TextInputFormat.class;
    Class<? extends WritableComparable> keyMapOutClass;
    Class<? extends Writable> valueMapOutClass;
    Class keyOutClass;
    Class<? extends Writable> valueOutClass;
    Class<? extends OutputFormat> outputFormatClass;

    Class<? extends Mapper> mapperClass = MapperProxy.class;
    Class<? extends Reducer> combinerClass = null;
    Class<? extends Reducer> reducerClass = ReducerProxy.class;

    int maxConcurrentMaps = 1000;
    Integer numReduceTasks = null;
    Class<? extends MapReduceDriver> driverClass;
    String jobName;
    String queueName;

    public static class IdentityMapper extends Mapper {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            try {
                HadoopTask.configure(context.getConfiguration());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class SwappingIdentityMapper extends IdentityMapper {
        @Override
        protected void map(Object key, Object value, Mapper.Context context) throws IOException, InterruptedException {
            super.map(value, key, context);
        }
    }

    public SELF setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
        return (SELF) this;
    }

    public SELF setMapper(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
        return (SELF) this;
    }

    public SELF setIdentityMapper() {
        this.mapperClass = IdentityMapper.class;
        return (SELF) this;
    }

    public SELF setSwappingIdentityMapper() {
        this.mapperClass = SwappingIdentityMapper.class;
        return (SELF) this;
    }

    public SELF setCombiner(Class<? extends Reducer> combinerClass) {
        this.combinerClass = combinerClass;
        return (SELF) this;
    }

    public SELF setReducer(Class<? extends Reducer> reducerClass) {
        this.reducerClass = reducerClass;
        return (SELF) this;
    }

    public SELF setMaxConcurrentMaps(Integer maxConcurrentMaps) {
        this.maxConcurrentMaps = maxConcurrentMaps;
        return (SELF) this;
    }

    public SELF setNumReduceTasks(Integer numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
        return (SELF) this;
    }

    public SELF setMapOutput(Class<? extends WritableComparable> keyMapOutClass, Class<? extends Writable> valueMapOutClass) {
        this.keyMapOutClass = keyMapOutClass;
        this.valueMapOutClass = valueMapOutClass;
        return (SELF) this;
    }

    public SELF setOutput(Class<? extends OutputFormat> outputFormatClass, Class keyOutClass, Class<? extends Writable> valueOutClass) {
        this.keyOutClass = keyOutClass;
        this.valueOutClass = valueOutClass;
        this.outputFormatClass = outputFormatClass;
        return (SELF) this;
    }

    public SELF setDriver(MapReduceDriver driver) {
        this.driverClass = driver.getClass();
        return (SELF) this;
    }

    public SELF setJobName(String jobName) {
        if(jobName != null)
            this.jobName = jobName;
        return (SELF) this;
    }

    public void setQueueName(String queueName) {
        if(queueName != null)
            this.queueName = queueName;
    }

    public void preConfigureJob(){}
    public void postConfigureJob(){}

    @Override
    public void configureJob() throws Exception {
        HadoopTask.configure(getConfiguration());

        if (driverClass == null){
            setDriver(this);
        }

        logger.info("Running pre-configuration procedures...");
        preConfigureJob();

        logger.info("Driver class is set to " + driverClass);
        logger.info("Configuring the job " + getJobName() + " ...");

        Configuration conf = getConfiguration();
        if (isUnconfigured(conf, Job.MAP_CLASS_ATTR)) {
            if (mapperClass == null){
                throw new RuntimeException("Mapper class must be set");
            }
            job.setMapperClass(mapperClass);
            if (isUnconfigured(conf, "mapreduce.job.running.map.limit")
                    || conf.getInt("mapreduce.job.running.map.limit", 0) == 0) {
                conf.setInt("mapreduce.job.running.map.limit", maxConcurrentMaps);
                logger.info("Set Mapper to " + mapperClass.getName() + " and running map limit to " + maxConcurrentMaps);
            }else {
                logger.info("Set Mapper to " + mapperClass.getName());
            }
        }

        if (combinerClass != null && isUnconfigured(conf, Job.COMBINE_CLASS_ATTR)) {
            job.setCombinerClass(combinerClass);
            logger.info("Set combiner to " + combinerClass.getName());
        }

        if (isUnconfigured(conf, Job.INPUT_FORMAT_CLASS_ATTR)) {
            if (inputFormatClass == null)
                throw new RuntimeException("Input format class must be set");
            job.setInputFormatClass(inputFormatClass);
            logger.info("Set input format to " + inputFormatClass.getName());
        }

        if (keyMapOutClass != null || isConfigured(conf, Job.MAP_OUTPUT_KEY_CLASS)) {
            if (isUnconfigured(conf, Job.MAP_OUTPUT_KEY_CLASS)) {
                job.setMapOutputKeyClass(keyMapOutClass);
                if (valueMapOutClass == null)
                    throw new RuntimeException("Map output value class must be set");

                job.setMapOutputValueClass(valueMapOutClass);
                logger.info("Set map output to (" + keyMapOutClass + ", " + valueMapOutClass + ")");
            }

            enableMapperOutputSnappyCompression();
            logger.info("Enabled snappy compression for Mapper output");

            if (isUnconfigured(conf, Job.REDUCE_CLASS_ATTR)) {
                if (reducerClass == null){
                    logger.warn("Reducer class is not set - disabling Reduce phase");
                    job.setNumReduceTasks(0);
                    numReduceTasks = null;
                }else {
                    job.setReducerClass(reducerClass);
                    logger.info("Set reducer to " + reducerClass);
                }
            }
            if (numReduceTasks != null) {
                job.setNumReduceTasks(numReduceTasks);
                logger.info("Set number of reducers to " + numReduceTasks);
            }
        } else{
            job.setNumReduceTasks(0);
            logger.info("Disabled reduce phase");
        }

        if (isUnconfigured(conf, Job.OUTPUT_KEY_CLASS)){
            if (keyOutClass != null) {
                job.setOutputKeyClass(keyOutClass);
                logger.info( "Set job output key to " + keyOutClass);
            } else {
                logger.warn( "Job output key class is not set. Disabling job Output...");
                LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            }
        }

        if (isUnconfigured(conf, Job.OUTPUT_VALUE_CLASS)){
            if (valueOutClass != null) {
                job.setOutputValueClass(valueOutClass);
                logger.info( "Set job output value to " + valueOutClass);
            }else {
                logger.warn( "Job output value class is not set. Disabling job Output...");
                LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            }
        }

        if (isUnconfigured(conf, Job.OUTPUT_FORMAT_CLASS_ATTR)) {
            if (outputFormatClass == null) {
                logger.warn( "Job output format class is not set. Using Lazy Text Output format...");
                LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            }else if (outputFormatClass == TextOutputFormat.class) {
                LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
                logger.info("Set output format to " + outputFormatClass + " (lazy)");
            }else{
                job.setOutputFormatClass(outputFormatClass);
                logger.info("Set output format to " + outputFormatClass);
            }
        }

        conf.setClass(IMapperHelper.MAPPER_HELPER_CONFIG, driverClass, IMapperHelper.class);
        conf.setClass(IReducerHelper.REDUCER_HELPER_CONFIG, driverClass, IReducerHelper.class);

        if (queueName != null) {
            super.setQueueName(queueName);
            logger.info("Using queue " + queueName);
        }

        logger.info("Running post-configuration procedures...");
        postConfigureJob();
        logger.info("Completed run-time configuration!");
    }

    public static boolean isUnconfigured(Configuration conf, String attr) {
        return StringUtils.isEmpty(conf.get(attr));
    }

    public static boolean isConfigured(Configuration conf, String attr) {
        return StringUtils.isNotEmpty(conf.get(attr));
    }


    @Override
    protected String getJobName() {
        String jobNameFromConf = getConfiguration().get(MRJobConfig.JOB_NAME);

        if (jobNameFromConf != null)
            return jobNameFromConf;

        return jobName != null ? jobName : driverClass.getSimpleName();
    }

    @Override
    public String toString() {
        return "MapReduceDriver{" +
                "inputFormatClass=" + inputFormatClass +
                ", keyMapOutClass=" + keyMapOutClass +
                ", valueMapOutClass=" + valueMapOutClass +
                ", keyOutClass=" + keyOutClass +
                ", valueOutClass=" + valueOutClass +
                ", outputFormatClass=" + outputFormatClass +
                ", mapperClass=" + mapperClass +
                ", combinerClass=" + combinerClass +
                ", reducerClass=" + reducerClass +
                ", maxConcurrentMaps=" + maxConcurrentMaps +
                ", numReduceTasks=" + numReduceTasks +
                ", driverClass=" + driverClass +
                ", jobName='" + jobName + '\'' +
                ", outputPath=" + outputPath +
                ", cleanArgs=" + Arrays.toString(cleanArgs) +
                ", skipTrash=" + skipTrash +
                '}';
    }
}
