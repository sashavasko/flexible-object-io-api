package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.util.InstanceFactory;

public class BatchInputConf extends InputConf<BatchInputConf> {

    public static final String SUBNAMESPACE = "input.batch";

    private Long keyStart;
    private Integer size;
    private Integer batchesPerSplit;
    private Integer batchesNum;
    private Class<? extends BatchInputSplit> splitClass;
    private String keyMaxDatasetPath;
    private String keyMaxDatasetColumnName;
    private Class<? extends MaxKeyCalculator> keyMaxCalculator;
    private Class<? extends BatchKeyManager> keyManager;
    private String keyColumnName;
    private Long reduceMaxKeys;

    public BatchInputConf() {
        super();
    }

    @Override
    public BatchInputConf setDefaults() {
        keyStart = 0l; //conf.getLong(CFX_BATCH_KEY_START, 0l);
        size = 1000; //conf.getInt(BatchInputSplit.CFX_BATCH_SIZE, 1000);
        batchesPerSplit = 200; //conf.getInt(BatchInputSplit.CFX_BATCHES_PER_SPLIT, 200);
        batchesNum = 15000; //conf.getInt(CFX_BATCHES_NUM, 15000);
        splitClass = BatchInputSplit.class; // conf.getClass(CFX_BATCH_SPLIT_CLASS, BatchInputSplit.class, BatchInputSplit.class);
        splitterClass = BatchSplitter.class;
        readerClass = BatchRecordReader.Long.class;
        keyMaxCalculator = ParquetMaxKeyCalculator.class;
        keyManager = BatchKeyManager.class;
        reduceMaxKeys = 16000000l;

        return this;
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

    public Class<? extends BatchInputSplit> getSplitClass() {
        return splitClass == null ? BatchInputSplit.class : splitClass;
    }
    public BatchInputSplit getSplit() throws IllegalAccessException, InstantiationException {
        BatchInputSplit splitInstance = InstanceFactory.get(getSplitClass());
        splitInstance.setConf(getConf());
        return splitInstance;
    }

    public String getKeyMaxDatasetPath() {
        return keyMaxDatasetPath;
    }

    public String getKeyMaxDatasetColumnName() {
        return keyMaxDatasetColumnName;
    }

    public Class<? extends MaxKeyCalculator> getKeyMaxCalculatorClass() {
        return keyMaxCalculator == null ? ParquetMaxKeyCalculator.class : keyMaxCalculator;
    }

    public MaxKeyCalculator getKeyMaxCalculator() throws IllegalAccessException, InstantiationException {
        MaxKeyCalculator maxKeyCalculatorInstance = InstanceFactory.get(getKeyMaxCalculatorClass());
        maxKeyCalculatorInstance.setConf(getConf());
        return maxKeyCalculatorInstance;
    }

    public Class<? extends BatchKeyManager> getKeyManagerClass() throws IllegalAccessException, InstantiationException {
        return keyManager == null ? BatchKeyManager.class : keyManager;
    }

    public BatchKeyManager getKeyManager() throws IllegalAccessException, InstantiationException {
        BatchKeyManager batchKeyManagerInstance = InstanceFactory.get(getKeyManagerClass());
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