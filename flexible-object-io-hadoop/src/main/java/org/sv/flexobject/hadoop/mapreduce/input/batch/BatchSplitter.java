package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.mapreduce.input.Splitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchSplitter implements Splitter {
    @Override
    public List<InputSplit> split(Configuration configuration) throws IOException {
        BatchInputConf conf = new BatchInputConf().from(configuration);

        long splitsCount = (conf.getBatchesNum() + conf.getBatchesPerSplit() - 1)/conf.getBatchesPerSplit();
        long maxControlNumber = conf.getKeyStart() + conf.getBatchesNum() * conf.getSize();

        List<InputSplit> splits = new ArrayList<>();
        long startKey = conf.getKeyStart();
        for (int i = 0 ; i < splitsCount ; i++){
            long batchesInSplit = conf.getBatchesPerSplit();
            if (maxControlNumber - startKey < conf.getBatchesPerSplit())
                batchesInSplit = maxControlNumber - startKey;

            try {
                BatchInputSplit split = conf.getSplit();
                split.setStartKey(startKey);
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
