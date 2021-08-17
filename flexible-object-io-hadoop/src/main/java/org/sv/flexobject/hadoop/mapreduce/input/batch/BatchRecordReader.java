package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.mapreduce.input.AdapterRecordReader;
import org.sv.flexobject.hadoop.mapreduce.input.DaoRecordReader;

import java.io.IOException;

abstract public class BatchRecordReader<VT> extends DaoRecordReader<LongWritable, VT, BatchInputDao, BatchInputSplit> {
    static Logger logger = Logger.getLogger(BatchRecordReader.class);

    long recordsRead = 0;
    long lastBatchRecordsRead = -1;// don't do the check for empty last batch on the first batch
    int batchNo = 0;
    long nextBatchStartKey = 0l;
    LongField key;
    boolean hasData = false;

    public static class Long extends BatchRecordReader<LongWritable> {
        AdapterRecordReader.LongField value;

        @Override
        protected LongWritable convertCurrentValue() throws Exception {
            if (value == null)
                value = longField();
            return value.convert(getValueFieldName());
        }
    }

    public static class Text extends BatchRecordReader<org.apache.hadoop.io.Text> {
        AdapterRecordReader.TextField value;

        @Override
        protected org.apache.hadoop.io.Text convertCurrentValue() throws Exception {
            if (value == null)
                value = textField();
            return value.convert(getValueFieldName());
        }
    }

    @Override
    protected LongWritable convertCurrentKey() throws Exception {
        if (key == null)
            key = longField();
        return key.convert(getKeyFieldName());
    }

    @Override
    protected InAdapter createAdapter(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return null;
    }

    protected void initializeSuper(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        super.initialize(split,context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        initializeSuper(split, context);
        nextBatchStartKey = getSplit().getStartKey();
        hasData = nextBatch();
        getProgressReporter().setSize(getSplit().getBatchPerSplit());
    }

    protected void incrementBatch(){
        batchNo++;
        getProgressReporter().increment();
    }

    protected boolean isEmptyBatch(){
        return lastBatchRecordsRead == recordsRead;
    }

    protected boolean hasMoreBatches(){
        return batchNo < getSplit().getBatchPerSplit();
    }

    protected boolean nextBatch(){
        long batchSize = getSplit().getBatchSize();
        long batchesPerSplit = getSplit().getBatchPerSplit();
        try {

            if (isEmptyBatch()) {
                logger.info("Empty batch encountered - checking if we need to skip a few ...");
                long adjustedKey = getDao().adjustStartKey(nextBatchStartKey, nextBatchStartKey + ((batchesPerSplit - batchNo) * batchSize));
                logger.info("Edjusted start key to " + adjustedKey);
                while (adjustedKey > nextBatchStartKey + batchSize && batchNo < batchesPerSplit) {
                    incrementBatch();
                    logger.info("Skipping batch " + batchNo);
                    nextBatchStartKey += batchSize;
                }
                if (!hasMoreBatches()) {
                    return false;
                }
            }

            incrementBatch();
            lastBatchRecordsRead = recordsRead;
            logger.info("Starting batch " + batchNo + " of " + batchesPerSplit + " with start key " + nextBatchStartKey + " records read so far " + recordsRead);

            setInput(getDao().startBatch(nextBatchStartKey, batchSize));

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
        if (!hasData())
            return false;
        try {
            if (getInput().next())
                return nextRecordFound();

            do {
                if (!hasMoreBatches())
                    return false;

                if (!nextBatch())
                    return noMoreData();

            }while (!getInput().next());
            return nextRecordFound();
        } catch (Exception e) {
            logger.error("Failed to get new record :", e);
            return false;
        }
    }

    public long getNextBatchStartKey() {
        return nextBatchStartKey;
    }

    protected boolean nextRecordFound(){
        ++recordsRead;
        return true;
    }

    protected boolean noMoreData(){
        hasData = false;
        return false;
    }

    public boolean hasData() {
        return hasData;
    }

    public int getBatchNo() {
        return batchNo;
    }
}
