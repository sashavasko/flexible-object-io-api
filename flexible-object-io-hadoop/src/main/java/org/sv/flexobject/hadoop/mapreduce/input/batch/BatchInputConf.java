package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public class BatchInputConf extends HadoopPropertiesWrapper<BatchInputConf> {

    public static final String SUBNAMESPACE = "input.batch";

    public Long keyStart = 0l; //conf.getLong(CFX_BATCH_KEY_START, 0l);
    public Integer size = 1000; //conf.getInt(BatchInputSplit.CFX_BATCH_SIZE, 1000);
    public Integer batchesPerSplit = 200; //conf.getInt(BatchInputSplit.CFX_BATCHES_PER_SPLIT, 200);
    public Integer batchesNum = 15000; //conf.getInt(CFX_BATCHES_NUM, 15000);
    public Class<? extends BatchInputSplit> splitClass = BatchInputSplit.class; // conf.getClass(CFX_BATCH_SPLIT_CLASS, BatchInputSplit.class, BatchInputSplit.class);
    public Class<? extends BatchRecordReader> readerClass = BatchRecordReader.Long.class;
    public String keyMaxDatasetPath;
    public String keyMaxDatasetColumnName;
    public Class<? extends MaxKeyCalculator> keyMaxCalculator = ParquetMaxKeyCalculator.class;
    public Class<? extends BatchKeyManager> keyManager = BatchKeyManager.class;
    public String keyColumnName;
    public Long reduceMaxKeys = 16000000l;


    public BatchInputConf() {
    }

    public BatchInputConf(String namespace) {
        super(namespace);
    }

    @Override
    public String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public void configureMaxKeyDataset(String maxKeyDatasetPath, String keyColumnName) {
        keyMaxDatasetPath = maxKeyDatasetPath;
        keyMaxDatasetColumnName = keyColumnName;
    }
}