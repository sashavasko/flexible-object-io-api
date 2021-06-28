package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.mapreduce.input.AdapterRecordReader;
import org.sv.flexobject.hadoop.mapreduce.input.DaoRecordReader;

import java.io.IOException;

abstract public class BatchRecordReader<VT>  extends DaoRecordReader<LongWritable, VT> {
    static Logger logger = Logger.getLogger(BatchRecordReader.class);

    long recordsRead = 0;
    long lastBatchRecordsRead = -1;// don't do the check for empty last batch on the first batch
    int batchNo = 0;
    long nextBatchStartKey = 0l;
    LongField key = new LongField();
    boolean hasData = false;

    public static class Long extends BatchRecordReader<LongWritable> {
        AdapterRecordReader.LongField value = new LongField();
        @Override
        protected LongWritable convertCurrentValue() throws Exception {
            return value.convert(valueFieldName);
        }
    }

    public static class Text extends BatchRecordReader<org.apache.hadoop.io.Text> {
        AdapterRecordReader.TextField value = new TextField();
        @Override
        protected org.apache.hadoop.io.Text convertCurrentValue() throws Exception {
            return value.convert(valueFieldName);
        }
    }

    @Override
    protected LongWritable convertCurrentKey() throws Exception {
        return key.convert(keyFieldName);
    }

    @Override
    protected InAdapter createAdapter(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return null;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        super.initialize(split,context);
        nextBatchStartKey = ((BatchInputSplit)split).getStartKey();
        hasData = nextBatch();
        getProgressReporter().setSize(((BatchInputSplit)split).getBatchPerSplit());
    }

    protected void incrementBatch(){
        batchNo++;
        getProgressReporter().increment();
    }

    protected boolean nextBatch(){
        long batchSize = ((BatchInputSplit)getSplit()).getBatchSize();
        long batchesPerSplit = ((BatchInputSplit)getSplit()).getBatchPerSplit();
        BatchInputDao inputDao = (BatchInputDao) dao;
        try {

            if (lastBatchRecordsRead == recordsRead) {
                logger.info("Empty batch encountered - checking if we need to skip a few ...");
                long adjustedKey = inputDao.adjustStartKey(nextBatchStartKey, nextBatchStartKey + ((batchesPerSplit - batchNo) * batchSize));
                while (adjustedKey > nextBatchStartKey + batchSize && batchNo < batchesPerSplit) {
                    incrementBatch();
                    logger.info("Skipping batch " + batchNo);
                    nextBatchStartKey += batchSize;
                }
                if (batchNo >= batchesPerSplit) {
                    return false;
                }
            }

            incrementBatch();
            lastBatchRecordsRead = recordsRead;
            logger.info("Starting batch " + batchNo + " of " + batchesPerSplit + " with start key " + nextBatchStartKey + " records read so far " + recordsRead);

            setInput(inputDao.startBatch(nextBatchStartKey, batchSize));

            nextBatchStartKey += batchSize;
            logger.info("Got input from DAO. Next Batch start key incremented to " + nextBatchStartKey);
            return true;
        } catch (Exception e) {
            logger.error("Failed to start a new Batch " + batchNo + " for a start key " + nextBatchStartKey, e);
            throw new RuntimeException("Failed to start a new Batch", e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!hasData)
            return false;
        try {
            if (getInput().next()) {
                recordsRead++;
                return true;
            }
            do {
                if (batchNo >= ((BatchInputSplit)getSplit()).getBatchPerSplit())
                    return false;

                if (!nextBatch()){
                    hasData = false;
                    return false;
                }

            }while (!getInput().next());
            ++recordsRead;
            return true;
        } catch (Exception e) {
            logger.error("Failed to get new record :", e);
            return false;
        }
    }
}
