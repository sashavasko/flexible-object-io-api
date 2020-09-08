package org.sv.flexobject.hadoop.mapreduce.util;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.HadoopBatchEnvironment;

import java.io.IOException;

public abstract class DaoRecordReader<KT,VT> extends RecordReader<KT,VT> {
    Logger logger = Logger.getLogger(DaoRecordReader.class);

//    public static final String CFX_DAO_READER_KEY_FIELD_NAME = "cfx.batch.key.field.name";
//    public static final String CFX_DAO_READER_VALUE_FIELD_NAME = "cfx.batch.value.field.name";
//    public static final String CFX_DAO_CLASS = "cfx.batch.dao.class";
//
    public static final String CURRENT_KEY_FIELD_NAME = "CURRENT_KEY";
    public static final String CURRENT_VALUE_FIELD_NAME = "CURRENT_VALUE";

    protected MRDao dao;
    protected InAdapter input;
    protected InputSplit split;
    protected String keyFieldName;
    protected String valueFieldName;

    public class LongField {
        LongWritable v = new LongWritable();

        public LongWritable convert(String fieldName) throws Exception {
            v.set(input.getLong(fieldName));
            return v;
        }
    }

    public class TextField {
        Text v = new Text();

        public Text convert(String fieldName) throws Exception {
            v.set(input.getString(fieldName));
            return v;
        }
    }

    protected void setupDao(TaskAttemptContext context) {
        DaoRecordReaderConf conf = new DaoRecordReaderConf().from(context.getConfiguration());
        Exception ee = null;
        if (conf.daoClass == null)
            throw new RuntimeException("Must specify a DAOclass extending MRDao using property " + conf.getSettingName("recordReaderDaoClass"));
        keyFieldName = conf.keyFieldName;
        valueFieldName = conf.valueFieldName;
        for (int i = 0; i < 10 ; i++){
            try {
                dao = conf.daoClass.newInstance();
                if (dao instanceof Configurable)
                    ((Configurable)dao).setConf(context.getConfiguration());
                return;
            } catch (Exception e) {
                ee = e;
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
            }
        }
        throw new RuntimeException("Failed to create DAO", ee);
    }

    protected void setInput(InAdapter input) throws Exception {
        if (this.input != null) {
            this.input.close();
        }

        this.input = input;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        HadoopBatchEnvironment.setConfiguration(context.getConfiguration());
        this.split = split;
        setupDao(context);
    }

    protected abstract KT convertCurrentKey() throws Exception;

    @Override
    public KT getCurrentKey() throws IOException, InterruptedException {
        try {
            return convertCurrentKey();
        } catch (Exception e) {
            logger.error("Failed to convert current key", e);
            return null;
        }
    }

    protected abstract VT convertCurrentValue() throws Exception;

    @Override
    public VT getCurrentValue() throws IOException, InterruptedException {
        try {
            return convertCurrentValue();
        } catch (Exception e) {
            logger.error("Failed to convert current value", e);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (input != null)
                input.close();
            dao.close();
        } catch (Exception e) {
            logger.error("Failed to close reader", e);
        }
    }

}
