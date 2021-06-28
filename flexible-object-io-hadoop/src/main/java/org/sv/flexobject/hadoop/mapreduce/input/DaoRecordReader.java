package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.mapreduce.util.MRDao;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public abstract class DaoRecordReader<KT,VT> extends AdapterRecordReader<KT,VT> {
    static Logger logger = Logger.getLogger(DaoRecordReader.class);

    public static final String CURRENT_KEY_FIELD_NAME = "CURRENT_KEY";
    public static final String CURRENT_VALUE_FIELD_NAME = "CURRENT_VALUE";
    public static final int DEFAULT_MAX_RETRIES_VALUE = 3;

    protected MRDao dao;
    protected String keyFieldName;
    protected String valueFieldName;

    public MRDao getDao() {
        return dao;
    }

    public String getKeyFieldName() {
        return keyFieldName;
    }

    public String getValueFieldName() {
        return valueFieldName;
    }

    @Override
    protected void setupInput(InputSplit split, TaskAttemptContext context) {
        DaoRecordReaderConf conf = InstanceFactory.get(DaoRecordReaderConf.class);
        conf.from(context.getConfiguration());

        Exception ee = null;
        if (!conf.isDaoConfigured())
            throw new RuntimeException("Must specify a DAO class extending MRDao using property " + conf.getSettingName("daoClass"));
        keyFieldName = conf.getKeyFieldName();
        valueFieldName = conf.getValueFieldName();
        for (int i = 0; i < conf.getMaxRetries() ; i++){
            try {
                dao = conf.createDao();
                if (dao instanceof Configurable)
                    ((Configurable)dao).setConf(context.getConfiguration());
                setInput(createAdapter(split, context));
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

    @Override
    public void close() throws IOException {
        super.close();
        try {
            if (dao != null)
                dao.close();
        } catch (Exception e) {
            logger.error("Failed to close DAO", e);
        }
    }
}
