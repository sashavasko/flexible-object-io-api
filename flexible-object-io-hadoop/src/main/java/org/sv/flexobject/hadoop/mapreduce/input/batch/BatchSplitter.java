package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.mapreduce.input.Splitter;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchSplitter implements Splitter {
    @Override
    public List<InputSplit> split(Configuration configuration) throws IOException {
        BatchInputConf conf = InstanceFactory.get(BatchInputConf.class);
        conf.from(configuration);

        long maxKey = conf.getMaxKey();

        List<InputSplit> splits = new ArrayList<>();
        long startKey = conf.getKeyStart();
        for (int i = 0 ; i < conf.getSplitsCount() ; i++){
            long batchesInSplit = conf.getBatchesPerSplit();

            if (maxKey - startKey < batchesInSplit*conf.getSize())
                batchesInSplit = (conf.getSize() - 1 + maxKey - startKey)/conf.getSize() ;

            try {
                BatchInputSplit split = conf.getSplit();
                split.setStartKey(startKey);
                split.setBatchPerSplit(batchesInSplit);
                splits.add(split);
            } catch (Exception e) {
                if (e instanceof IOException)
                    throw (IOException)e;
                throw new IOException(conf.addDiagnostics("Failed to instantiate split"), e);
            }
            startKey += conf.getSize() * batchesInSplit;
        }
        return splits;
    }
}
