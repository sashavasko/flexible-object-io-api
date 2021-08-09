package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetUtils;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public class ParquetMaxKeyCalculator implements MaxKeyCalculator {

    Logger logger = Logger.getLogger(ParquetMaxKeyCalculator.class);

    BatchInputConf inputConf = InstanceFactory.get(BatchInputConf.class);

    public ParquetMaxKeyCalculator() {
    }

    public ParquetMaxKeyCalculator(String maxKeyDatasetPath, String keyColumnName) {
        inputConf.configureMaxKeyDataset(maxKeyDatasetPath, keyColumnName);
    }

    @Override
    public void reconfigure() {
        inputConf.from(getConf());
    }

    @Override
    public Comparable calculateMaxKey() throws IOException {
        logger.info("Calculating max Key in " + inputConf.getKeyMaxDatasetPath() + " ...");
        return ParquetUtils.getMaxValueInFiles(getConf(), new Path(inputConf.getKeyMaxDatasetPath()), true, inputConf.getKeyMaxDatasetColumnName());
    }
}
