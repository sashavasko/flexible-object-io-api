package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public class BatchInputConf extends HadoopPropertiesWrapper<BatchInputConf> {

    public static final String SUBNAMESPACE = "input.batch";

    private Long keyStart = 0l; //conf.getLong(CFX_BATCH_KEY_START, 0l);
    private Integer size = 1000; //conf.getInt(BatchInputSplit.CFX_BATCH_SIZE, 1000);
    private Integer batchesPerSplit = 200; //conf.getInt(BatchInputSplit.CFX_BATCHES_PER_SPLIT, 200);
    private Integer batchesNum = 15000; //conf.getInt(CFX_BATCHES_NUM, 15000);
    private Class<? extends BatchInputSplit> splitClass = BatchInputSplit.class; // conf.getClass(CFX_BATCH_SPLIT_CLASS, BatchInputSplit.class, BatchInputSplit.class);
    private Class<? extends BatchRecordReader> readerClass = BatchRecordReader.Long.class;
    private String keyMaxDatasetPath;
    private String keyMaxDatasetColumnName;
    private Class<? extends MaxKeyCalculator> keyMaxCalculator = ParquetMaxKeyCalculator.class;
    private Class<? extends BatchKeyManager> keyManager = BatchKeyManager.class;
    private String keyColumnName;
    private Long reduceMaxKeys = 16000000l;


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

    public void setKeyColumnName(String keyColumnName) {
        this.keyColumnName = keyColumnName;
    }

    public void setKeyStart(Long keyStart) {
        this.keyStart = keyStart;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public long getKeyStart() {
        return keyStart == null ? 0l : keyStart;
    }

    public int getSize() {
        return size == null ? 1000 : size;
    }

    public int getBatchesPerSplit() {
        return batchesPerSplit == null ? 200 : batchesPerSplit;
    }

    public int getBatchesNum() {
        return batchesNum == null ? 1500 : batchesNum;
    }

    public BatchInputSplit getSplit() throws IllegalAccessException, InstantiationException {
        BatchInputSplit splitInstance = splitClass == null ? new BatchInputSplit() : splitClass.newInstance();
        splitInstance.setConf(getConf());
        return splitInstance;
    }

    public BatchRecordReader getReader() throws IllegalAccessException, InstantiationException {
        return readerClass == null ? new BatchRecordReader.Long() : readerClass.newInstance();
    }

    public String getKeyMaxDatasetPath() {
        return keyMaxDatasetPath;
    }

    public String getKeyMaxDatasetColumnName() {
        return keyMaxDatasetColumnName;
    }

    public MaxKeyCalculator getKeyMaxCalculator() throws IllegalAccessException, InstantiationException {
        MaxKeyCalculator maxKeyCalculatorInstance = keyMaxCalculator == null ? new ParquetMaxKeyCalculator() : keyMaxCalculator.newInstance();
        maxKeyCalculatorInstance.setConf(getConf());
        return maxKeyCalculatorInstance;
    }

    public BatchKeyManager getKeyManager() throws IllegalAccessException, InstantiationException {
        BatchKeyManager batchKeyManagerInstance = keyManager == null ? new BatchKeyManager() : keyManager.newInstance();
        batchKeyManagerInstance.setConf(getConf());
        return batchKeyManagerInstance;
    }

    public String getKeyColumnName() {
        return keyColumnName;
    }

    public long getReduceMaxKeys() {
        return reduceMaxKeys == null ? 16000000l : reduceMaxKeys;
    }

    public void setBatchesNum(int batchesNum) {
        this.batchesNum = batchesNum;
    }
}