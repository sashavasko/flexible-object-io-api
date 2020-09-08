package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.sv.flexobject.hadoop.mapreduce.util.DaoRecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

abstract public class BatchRecordReader<VT> extends DaoRecordReader<LongWritable, VT> {
    int batchNo = 0;
    long nextBatchStartControlNumber = 0l;
    LongField key = new LongField();

    public static class Long extends BatchRecordReader<LongWritable> {
        DaoRecordReader.LongField value = new LongField();
        @Override
        protected LongWritable convertCurrentValue() throws Exception {
            return value.convert(valueFieldName);
        }
    }

    public static class Text extends BatchRecordReader<org.apache.hadoop.io.Text> {
        DaoRecordReader.TextField value = new TextField();
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
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        super.initialize(split,context);
        nextBatchStartControlNumber = ((BatchInputSplit)split).getStartKey();
        nextBatch();
    }

    protected void nextBatch(){
        try {
            setInput(((BatchInputDao)dao).startBatch(nextBatchStartControlNumber, ((BatchInputSplit)split).getBatchSize()));
            nextBatchStartControlNumber += ((BatchInputSplit)split).getBatchSize();
            batchNo++;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            if (input.next())
                return true;
            do {
                if (batchNo >= ((BatchInputSplit)split).getBatchPerSplit())
                    return false;

                nextBatch();
            }while (!input.next());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return batchNo;
    }

}
