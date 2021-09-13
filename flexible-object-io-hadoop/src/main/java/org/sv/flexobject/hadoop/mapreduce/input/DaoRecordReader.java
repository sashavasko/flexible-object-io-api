package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.mapreduce.util.MRDao;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public abstract class DaoRecordReader<KT,VT,DAO extends MRDao,SPLIT extends InputSplit> extends AdapterRecordReader<KT,VT,SPLIT> {
    protected static Logger logger = Logger.getLogger(DaoRecordReader.class);

    public static final String CURRENT_KEY_FIELD_NAME = "CURRENT_KEY";
    public static final String CURRENT_VALUE_FIELD_NAME = "CURRENT_VALUE";
    public static final int DEFAULT_MAX_RETRIES_VALUE = 3;

    protected DAO dao;
    protected String keyFieldName;
    protected String valueFieldName;
    DaoRecordReaderConf conf;

    public DAO getDao() {
        return dao;
    }

    public String getKeyFieldName() {
        return keyFieldName;
    }

    public String getValueFieldName() {
        return valueFieldName;
    }

    public DaoRecordReaderConf getConf() {
        return conf;
    }

    @Override
    public void setupInput(InputSplit split, TaskAttemptContext context) {
        conf = InstanceFactory.get(DaoRecordReaderConf.class);
        conf.from(context.getConfiguration());

        Exception ee = null;
        if (!conf.isDaoConfigured())
            throw conf.runtimeException(logger, "Must specify a DAO class extending MRDao using property " + conf.getSettingName("daoClass"), null);
        keyFieldName = conf.getKeyFieldName();
        valueFieldName = conf.getValueFieldName();
        for (int i = 0; i < conf.getMaxRetries() ; i++){
            try {
                dao = (DAO) conf.createDao();
                if (dao instanceof Configurable)
                    ((Configurable)dao).setConf(context.getConfiguration());
                setInput(createAdapter(split, context));
                logger.info("Created record reader for split: " + split.toString() + " using configuration: " + conf.toString());
                return;
            } catch (Exception e) {
                ee = e;
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw conf.runtimeException(logger, "Interrupted while creating DAO", e);
            }
        }
        throw conf.runtimeException(logger, "Failed to create DAO", ee);
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
