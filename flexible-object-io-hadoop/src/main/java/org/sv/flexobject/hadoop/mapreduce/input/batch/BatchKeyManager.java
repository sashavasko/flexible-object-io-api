package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetUtils;
import org.sv.flexobject.hadoop.utils.FileSystemUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.TreeMap;

public class BatchKeyManager extends Configured implements Tool {
    public static final String JOB_SIZE_FILE_NAME = "_jobSize";

    Logger logger = Logger.getLogger(BatchKeyManager.class);

    long maxKeysToProcess = 320000000l;
    long startKey;
    long endKey;

    MaxKeyCalculator maxKeyCalculator;
    BatchInputConf inputConf = new BatchInputConf();

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf != null){
            inputConf.from(conf);
            MaxKeyCalculator maxKeyCalculator = null;
            try {
                maxKeyCalculator = inputConf.keyMaxCalculator.newInstance();
            } catch (Exception e) {
                logger.error("Cannot get max key Calculator", e);
            }
            if (maxKeyCalculator != null) {
                this.maxKeyCalculator = maxKeyCalculator;
            }else if (this.maxKeyCalculator != null){
                this.maxKeyCalculator.setConf(conf);
            }
            BatchInputConf batchInputConf = new BatchInputConf().from(conf);
            maxKeysToProcess = batchInputConf.batchesNum * batchInputConf.size;
        }
    }

    public BatchKeyManager(Configuration conf, long maxKeysToProcess, String keyColumnName, MaxKeyCalculator maxKeyCalculator) {
        super(conf);
        this.maxKeysToProcess = maxKeysToProcess;
        inputConf.keyColumnName = keyColumnName;
        this.maxKeyCalculator = maxKeyCalculator;
    }

    public BatchKeyManager(String keyColumnName, MaxKeyCalculator maxKeyCalculator) {
        inputConf.keyColumnName = keyColumnName;
    }

    public BatchKeyManager(Configuration conf) {
        super(conf);
    }

    public BatchKeyManager() {
    }

    public class Extract {

        Path path;
        long filesCount = 0;
        long totalSizeBytes = 0;
        long startKey;
        Long endKey;
        Long jobSize;

        public Extract(Path path) throws IOException {
            this.path = path;
            analyze();
        }

        private void analyze() throws IOException {
            logger.info("Analyzing extracts in " + path + " ...");
            FileSystem fs = path.getFileSystem(getConf());
            RemoteIterator<LocatedFileStatus> iter = fs.listFiles(path, false);
            while (iter.hasNext()) {
                LocatedFileStatus lfs = iter.next();
                if (lfs.getPath().getName().endsWith(".parquet")) {
                    logger.debug("Checking " + lfs);
                    filesCount++;
                    totalSizeBytes += lfs.getLen();
                }
            }
            startKey = Long.valueOf(path.getName());
            if (filesCount > 0)
                endKey = (long) ParquetUtils.getMaxValueInFiles(getConf(), path, true, inputConf.keyColumnName);
            jobSize = FileSystemUtils.readLongFromFile(getConf(), new Path(path, JOB_SIZE_FILE_NAME));
        }

        long getAverageFileSizeMB(){
            return filesCount > 0 ? totalSizeBytes/(filesCount * 1024 * 1024) : 0;
        }

        public Path getPath() {
            return path;
        }

        public long getFilesCount() {
            return filesCount;
        }

        public long getTotalSizeBytes() {
            return totalSizeBytes;
        }

        public long getStartKey() {
            return startKey;
        }

        // These may return null!!!!
        public Long getEndKey() {
            return endKey;
        }

        public Long getJobSize() {
            return jobSize;
        }

        @Override
        public String toString() {
            return "Extract{" +
                    "path=" + path +
                    ", filesCount=" + filesCount +
                    ", totalSizeBytes=" + totalSizeBytes +
                    ", startKey=" + startKey +
                    ", endKey=" + endKey +
                    ", average file size=" + getAverageFileSizeMB() + "MB"+
                    '}';
        }
    }

    long maxKey;
    TreeMap<Long, Extract> extractFolders = new TreeMap<>();


    public void setMaxKey(long maxKey) {
        this.maxKey = maxKey;
    }

    protected Long calculateMaxKey() throws IOException {
        if (maxKeyCalculator != null) {
            setMaxKey((Long) maxKeyCalculator.calculateMaxKey());
        } else {
            logger.info("Max Key Calculator is not set - assuming no upper bound ...");
            setMaxKey(Long.MAX_VALUE);
        }
        return maxKey;
    }

    protected Extract collectExtracts(Path rootFolder) throws IOException {
        logger.info("Listing extracts in " + rootFolder + " ...");
        FileSystem fs = rootFolder.getFileSystem(getConf());
        RemoteIterator<LocatedFileStatus> iter = null;
        try {
            iter = fs.listLocatedStatus(rootFolder);
            while (iter.hasNext()) {
                LocatedFileStatus lfs = iter.next();
                logger.debug("Checking " + lfs + (lfs.isDirectory() ? " is dir" : " not dir") + (StringUtils.isNumeric(lfs.getPath().getName()) ? " is numeric" : " is not numeric"));
                if (lfs.isDirectory() && StringUtils.isNumeric(lfs.getPath().getName())) {
                    logger.debug(" checking if has _SUCCESS file in it");
                    if (fs.isFile(new Path(lfs.getPath(), "_SUCCESS"))) {
                        long startKey = Long.valueOf(lfs.getPath().getName());
                        extractFolders.put(startKey, new Extract(lfs.getPath()));
                    } else {
                        logger.debug("no _SUCCESS file. Ignoring");
                    }
                }
            }
        }catch (FileNotFoundException e){
            return null;
        }
        return extractFolders.isEmpty() ? null : extractFolders.lastEntry().getValue();
    }

    protected long validateStartKey(long startKey){
        return startKey;
    }

    protected long validateEndKey(long endKey){
        return endKey;
    }

    protected void calculateRange(){
        Extract lastExtract = getLastExtract();
        if (lastExtract == null){
            startKey = 0;
            endKey = maxKeysToProcess;
        }else {
            long lastJobEndTct = lastExtract.getStartKey() + maxKeysToProcess;
            if (lastExtract.getJobSize() != null){
                lastJobEndTct = lastExtract.getStartKey() + lastExtract.getJobSize();
            }else if (lastExtract.getEndKey() != null){
                lastJobEndTct = lastExtract.getEndKey();
            }

            if (lastJobEndTct < getMaxKey()) {
                startKey = validateStartKey(lastJobEndTct);
                endKey = validateEndKey(startKey + maxKeysToProcess);
            } else {
                startKey = lastExtract.getStartKey();
                endKey = getMaxKey() + 1;
            }
        }
    }

    public void collectData(Path rootFolder) throws IOException {
        calculateMaxKey();
        collectExtracts(rootFolder);
        calculateRange();
    }

    public long getMaxKey() {
        return maxKey;
    }

    public Extract getLastExtract(){
        return extractFolders.isEmpty() ? null : extractFolders.lastEntry().getValue();
    }

    public long getStartKey() {
        return startKey;
    }

    public long getEndKey() {
        return endKey;
    }

    public void setKeyColumnName(String keyColumnName) {
        inputConf.keyColumnName = keyColumnName;
    }

    public void setMaxKeyCalculator(MaxKeyCalculator maxKeyCalculator) {
        this.maxKeyCalculator = maxKeyCalculator;
    }

    @Override
    public int run(String[] args) throws Exception {

        setKeyColumnName("textDecoderId");
        setMaxKeyCalculator(new ParquetMaxKeyCalculator("/decoder/tctToDecoderFull/1", "textDecoderId"));

        logger.setLevel(Level.DEBUG);
        collectData(new Path("/decoder/totalEncounteredCount/1/20200101"));
        logger.info("\nList of all extracts : \n" + extractFolders);
        logger.info("\nMost recent extract is " + getLastExtract());
        logger.info("Max   key : " + getMaxKey());
        logger.info("Start key : " + getStartKey());
        logger.info("End   key : " + getEndKey());

        return 0;
    }
}
