package org.sv.flexobject.hadoop.mapreduce.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.mapreduce.input.batch.BatchInputConf;
import org.sv.flexobject.hadoop.mapreduce.input.batch.BatchInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.batch.BatchKeyManager;
import org.sv.flexobject.hadoop.mapreduce.util.driver.OutputProducer;
import org.sv.flexobject.hadoop.mapreduce.util.driver.ParquetMapReduceDriver;
import org.sv.flexobject.hadoop.utils.FileSystemUtils;

import java.io.IOException;

public abstract class IncrementalExtract extends ParquetMapReduceDriver {
    Logger logger = Logger.getLogger(IncrementalExtract.class);

    protected Path outputParent;
    protected Path outputPath;
    protected long startKey;
    protected long endKey;

    protected long jobSize;

    BatchInputConf inputConf = new BatchInputConf();

    public IncrementalExtract() {
        setInputFormatClass(BatchInputFormat.Long.class);
        setIdentityMapper();
        setMapOutput(LongWritable.class, LongWritable.class);
        setQueueName("Production.VHDB_Pool");
        setMaxConcurrentMaps(200);
        setNumReduceTasks(1);
   }

    public BatchKeyManager makeKeyManager(Configuration conf) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return inputConf.getKeyManager();
    }

    @Override
    abstract public void reduce(WritableComparable keyIn, Iterable<Writable> values, Reducer.Context context) throws IOException, InterruptedException;

    @Override
    public void preConfigureJob() {
        Configuration conf = getConfiguration();
        inputConf.from(conf);
        long batchSize = inputConf.getSize();
        long batchNum = inputConf.getBatchesNum();
        long keysToProcess = batchSize * batchNum;

        jobSize = keysToProcess;

        BatchKeyManager keyManager = null;
        try {
            keyManager = makeKeyManager(conf);
            keyManager.collectData(outputParent);
        } catch (Exception e) {
            logger.error("Failed to collect data for previous extracts :", e);
            System.exit(1);
        }
        startKey = keyManager.getStartKey();
        logger.info("Adjusted start Key to " + startKey);
        endKey = keyManager.getEndKey();
        inputConf.setKeyStart(startKey);
        keysToProcess = endKey - startKey;
        batchNum = (keysToProcess + batchSize - 1)/batchSize;
        inputConf.setBatchesNum((int)batchNum);
        logger.info("Adjusted last Key to " + endKey + " and batch num to " + batchNum);
        logger.info("extracting from " + startKey + " to " + endKey);

        try {
            inputConf.update(conf);
        } catch (Exception e) {
            logger.error("Failed to configure batch parameters", e);
            throw new RuntimeException(e);
        }

        outputPath = compileOutputPath();
        logger.info("Writing output to " + outputParent.toString());
        super.defineOutput(outputPath);
        try {
            super.cleanupOutput();
        } catch (IOException e) {
            logger.error("Failed to cleanup output", e);
            throw new RuntimeException(e);
        }

        long numReduces = (keysToProcess + inputConf.getReduceMaxKeys() - 1)/ inputConf.getReduceMaxKeys(); // should produce output just under 128MB        logger.info("Using " + numReduces + " reduces.");
        logger.info("Using " + numReduces + " reduces.");
        setNumReduceTasks((int) numReduces);
        super.preConfigureJob();
    }

    public static Path compileOutputPath(Path outputParent, long startKey, long endKey) {
        return new Path(outputParent, String.format("%020d", startKey));
//        return new Path(outputParent, String.format("%020d-%020d", startKey, endKey));
    }

    protected Path compileOutputPath(){
        return compileOutputPath(outputParent, startKey, endKey);
    }

    @Override
    public OutputProducer defineOutput(Path output) {
        outputParent = output;
        return this;
    }

    @Override
    public void cleanupOutput() throws IOException {
        // our output is handled at a later stage after configuration is processed
    }

    protected Path makeLastKeyPath() {
        return new Path(outputParent, "_endKey");
    }
    protected Path makeJobSizePath() {
        return new Path(outputPath, BatchKeyManager.JOB_SIZE_FILE_NAME);
    }

    protected void saveLastKey(Configuration conf){
        Path lastPath = makeLastKeyPath();
        try{
            FileSystemUtils.writeLongToFile(conf, lastPath, endKey);
            logger.info("Saved endKey to " + lastPath);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    protected void saveJobSize(Configuration conf){
        Path lastPath = makeJobSizePath();
        try{
            FileSystemUtils.writeLongToFile(conf, lastPath, jobSize);
            logger.info("Saved jobSize " + jobSize + " to " + lastPath);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void map(WritableComparable keyIn, Writable valueIn, Mapper.Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void handleJobSuccess() {
        saveLastKey(getConfiguration());
        saveJobSize(getConfiguration());
        super.handleJobSuccess();
    }
}
