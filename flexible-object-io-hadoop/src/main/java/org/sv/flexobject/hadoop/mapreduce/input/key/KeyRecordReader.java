package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.mapreduce.input.DaoRecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

abstract public class KeyRecordReader<KT, VT> extends DaoRecordReader<KT, VT> {
    Logger logger = Logger.getLogger(KeyRecordReader.class);

    public static class LongLong extends KeyRecordReader<LongWritable, LongWritable> {
        DaoRecordReader.LongField key = new LongField();
        DaoRecordReader.LongField value = new LongField();

        @Override
        protected LongWritable convertCurrentKey() throws Exception {
            return key.convert(keyFieldName);
        }

        @Override
        protected LongWritable convertCurrentValue() throws Exception {
            return value.convert(valueFieldName);
        }
    }

    public static class LongText extends KeyRecordReader<LongWritable, Text> {
        DaoRecordReader.LongField key = new LongField();
        DaoRecordReader.TextField value = new TextField();

        @Override
        protected LongWritable convertCurrentKey() throws Exception {
            return key.convert(keyFieldName);
        }

        @Override
        protected Text convertCurrentValue() throws Exception {
            return value.convert(valueFieldName);
        }
    }

    @Override
    protected InAdapter createAdapter(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        try {
            return ((KeyInputDao) dao).start(((KeyInputSplit) split).getKey());
        } catch (Exception e) {
            if (e instanceof IOException)
                throw (IOException) e;
            throw new IOException("Failed to query DAO by the key", e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            if (input.next()) {
                progressReporter.increment();
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.error("Failed to get next record", e);
            return false;
        }
    }
}