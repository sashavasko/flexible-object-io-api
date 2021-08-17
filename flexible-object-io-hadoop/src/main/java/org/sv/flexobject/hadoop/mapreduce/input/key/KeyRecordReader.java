package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.mapreduce.input.AdapterRecordReader;
import org.sv.flexobject.hadoop.mapreduce.input.DaoRecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

abstract public class KeyRecordReader<KT, VT> extends DaoRecordReader<KT, VT, KeyInputDao,KeyInputSplit> {
    public static final Logger logger = Logger.getLogger(KeyRecordReader.class);

    public static class LongLong extends KeyRecordReader<LongWritable, LongWritable> {
        AdapterRecordReader.LongField key;
        AdapterRecordReader.LongField value;

        @Override
        protected LongWritable convertCurrentKey() throws Exception {
            if (key == null)
                key = longField();
            return key.convert(getKeyFieldName());
        }

        @Override
        protected LongWritable convertCurrentValue() throws Exception {
            if (value == null)
                value = longField();
            return value.convert(getValueFieldName());
        }
    }

    public static class LongText extends KeyRecordReader<LongWritable, Text> {
        AdapterRecordReader.LongField key;
        AdapterRecordReader.TextField value;

        @Override
        protected LongWritable convertCurrentKey() throws Exception {
            if (key == null)
                key = longField();
            return key.convert(getKeyFieldName());
        }

        @Override
        protected Text convertCurrentValue() throws Exception {
            if (value == null)
                value = textField();
            return value.convert(getValueFieldName());
        }
    }

    @Override
    protected InAdapter createAdapter(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        try {
            return getDao().start(getSplit().getKey());
        } catch (Exception e) {
            if (e instanceof IOException) {
                logger.error(getConf().addDiagnostics("Failed to query DAO by the key"), e);
                throw (IOException) e;
            }
            throw new IOException(getConf().addDiagnostics("Failed to query DAO by the key"), e);
        }
    }

    @Override
    public boolean nextKeyValue() {
        try {
            if (getInput().next()) {
                getProgressReporter().increment();
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.error("Failed to get next record", e);
            return false;
        }
    }
}