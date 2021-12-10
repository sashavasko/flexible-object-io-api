package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.hadoop.mapreduce.output.parquet.StreamableParquetOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.mapreduce.MRJobConfig.JOB_RUNNING_MAP_LIMIT;

abstract public class MapReduceDriver<SELF extends MapReduceDriver> extends ConfiguredDriver<SELF> implements IMapperHelper, IReducerHelper {

    Logger logger = Logger.getLogger(MapReduceDriver.class);

    Class<? extends InputFormat> inputFormatClass = TextInputFormat.class;
    Class<? extends WritableComparable> keyMapOutClass;
    Class<? extends Writable> valueMapOutClass;

    public static class FullyDefinedOutputFormat extends StreamableWithSchema {
        String name;
        Class keyClass;
        Class valueClass;
        Class<? extends OutputFormat> formatClass;

        public FullyDefinedOutputFormat(Class keyClass, Class valueClass, Class<? extends OutputFormat> formatClass) {
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.formatClass = formatClass;
        }

        public FullyDefinedOutputFormat(String name, Class keyClass, Class valueClass, Class<? extends OutputFormat> formatClass) {
            this.name = name;
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.formatClass = formatClass;
        }

        public Class getKeyClass() {
            return keyClass;
        }

        public Class<? extends Writable> getValueClass() {
            return valueClass;
        }

        public Class<? extends OutputFormat> getFormatClass() {
            return formatClass;
        }

        public String getName() {
            return name;
        }
    }

    List<FullyDefinedOutputFormat> outputs = new ArrayList<>();

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
        outputs.clear();
        outputs.add(new FullyDefinedOutputFormat(keyOutClass, valueOutClass, outputFormatClass));
        return (SELF) this;
    }

    public SELF addOutput(String name, Class<? extends OutputFormat> outputFormatClass, Class keyOutClass, Class valueOutClass) {
        outputs.add(new FullyDefinedOutputFormat(name, keyOutClass, valueOutClass, outputFormatClass));
        return (SELF) this;
    }

    public SELF addParquetOutput(String name, Class<? extends Streamable> valueOutClass) {
        outputs.add(new FullyDefinedOutputFormat(name, Void.class, valueOutClass, StreamableParquetOutputFormat.class));
        return (SELF) this;
    }

    public SELF addParquetOutput(String name, Class<? extends OutputFormat> outputFormatClass, Class<? extends Streamable> valueOutClass) {
        outputs.add(new FullyDefinedOutputFormat(name, Void.class, valueOutClass, outputFormatClass));
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
            if (isUnconfigured(conf, JOB_RUNNING_MAP_LIMIT)
                    || conf.getInt(JOB_RUNNING_MAP_LIMIT, 0) == 0) {
                conf.setInt(JOB_RUNNING_MAP_LIMIT, maxConcurrentMaps);
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

        if (outputs.size() > 1) {
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
            for (FullyDefinedOutputFormat output : outputs) {
                MultipleOutputs.addNamedOutput(job, output.getName(), output.getFormatClass(), output.getKeyClass(), output.getValueClass());
            }
            MultipleOutputs.setCountersEnabled(job, true);
        } else {
            if (isUnconfigured(conf, Job.OUTPUT_KEY_CLASS)) {
                if (outputs.size() == 1) {
                    job.setOutputKeyClass(outputs.get(0).getKeyClass());
                    logger.info("Set job output key to " + outputs.get(0).getKeyClass());
                } else {
                    logger.warn("Job output key class is not set. Disabling job Output...");
                    setOutputToNoOutput();
                }
            }

            if (isUnconfigured(conf, Job.OUTPUT_VALUE_CLASS)) {
                if (outputs.size() == 1) {
                    job.setOutputValueClass(outputs.get(0).getValueClass());
                    logger.info("Set job output value to " + outputs.get(0).getValueClass());
                } else {
                    logger.warn("Job output value class is not set. Disabling job Output...");
                    setOutputToNoOutput();
                }
            }

            if (isUnconfigured(conf, Job.OUTPUT_FORMAT_CLASS_ATTR)) {
                if (outputs.size() == 0) {
                    logger.warn("Job output format class is not set. Disabling output...");
                    setOutputToNoOutput();
                } else {
                    if (outputs.get(0).getFormatClass() == TextOutputFormat.class) {
                        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
                        logger.info("Set output format to " + outputs.get(0).getFormatClass() + " (lazy)");
                    } else {
                        logger.info("Set output format to " + outputs.get(0).getFormatClass());
                    }
                    job.setOutputFormatClass(outputs.get(0).getFormatClass());
                }
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
        if (StringUtils.isEmpty(conf.get(attr)))
            return true;
        String[] sources = conf.getPropertySources(attr);
        return (sources[0].contains("-site.xml") || sources[0].contains("-default.xml"));
    }

    public static boolean isConfigured(Configuration conf, String attr) {
        return !isUnconfigured(conf, attr);
    }

    @Override
    protected String getJobName() {
        String jobNameFromConf = getConfiguration().get(MRJobConfig.JOB_NAME);

        if (jobNameFromConf != null)
            return jobNameFromConf;

        return jobName != null ? jobName
                : driverClass != null ? driverClass.getSimpleName()
                : getClass().getSimpleName();
    }

    public List<FullyDefinedOutputFormat> getOutputs() {
        return outputs;
    }

    public boolean hasMultipleOutputs(){
        return outputs.size() > 1;
    }

    @Override
    public String toString() {
        return "MapReduceDriver{" +
                "inputFormatClass=" + inputFormatClass +
                ", keyMapOutClass=" + keyMapOutClass +
                ", valueMapOutClass=" + valueMapOutClass +
                ", outputs=" + outputs.toString() +
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
