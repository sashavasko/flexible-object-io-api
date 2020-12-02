package org.sv.flexobject.hadoop.mapreduce.util;

import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public class DaoRecordReaderConf extends HadoopPropertiesWrapper<DaoRecordReaderConf> {
    public static final String SUBNAMESPACE = "record.reader";

    protected String keyFieldName = DaoRecordReader.CURRENT_KEY_FIELD_NAME;
    protected String valueFieldName = DaoRecordReader.CURRENT_VALUE_FIELD_NAME;
    protected int maxRetries = DaoRecordReader.DEFAULT_MAX_RETRIES_VALUE;
    Class<? extends MRDao> daoClass;

    public DaoRecordReaderConf() {
    }

    public DaoRecordReaderConf(String namespace) {
        super(namespace);
    }

    @Override
    public String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public MRDao createDao() throws IllegalAccessException, InstantiationException {
        return daoClass.newInstance();
    }

    public String getKeyFieldName() {
        return keyFieldName;
    }

    public String getValueFieldName() {
        return valueFieldName;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

}