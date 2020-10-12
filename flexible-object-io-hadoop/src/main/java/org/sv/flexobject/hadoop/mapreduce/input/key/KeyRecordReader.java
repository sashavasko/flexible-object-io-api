package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.mapreduce.util.DaoRecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

abstract public class KeyRecordReader<KT, VT> extends DaoRecordReader<KT, VT> {
    Logger logger = Logger.getLogger(KeyRecordReader.class);
    long recordsRead = 0;
    long estimatedMaxRecords = 1000;

    public static class LongLong extends KeyRecordReader<LongWritable, LongWritable> {
        DaoRecordReader.LongField key = new LongField();
        DaoRecordReader.LongField value = new LongField();

        @Override
        protected LongWritable convertCurrentKey() throws Exception {
            return key.convert(keyFieldName);
        }

        @Override
        protected LongWritable convertCurrentValue() throws Exception { return value.convert(valueFieldName); }
    }

    public static class LongText extends KeyRecordReader<LongWritable, Text> {
        DaoRecordReader.LongField key = new LongField();
        DaoRecordReader.TextField value = new TextField();

        @Override
        protected LongWritable convertCurrentKey() throws Exception { return key.convert(keyFieldName); }

        @Override
        protected Text convertCurrentValue() throws Exception { return value.convert(valueFieldName); }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            if (input == null)
                setInput(((KeyInputDao)dao).start(((KeyInputSplit)split).getKey()));
            if (input.next()) {
                recordsRead++;
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.error("Failed to get next record", e);
            return false;
        }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (recordsRead >= estimatedMaxRecords*3/4)
            estimatedMaxRecords *= 2;
        return ((float)recordsRead)/((float)estimatedMaxRecords);
    }
}
