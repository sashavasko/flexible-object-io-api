package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.util.ArrayList;
import java.util.List;

public class ModSplitter implements KeySplitter {

    @Override
    public List<InputSplit> split(Configuration conf) {
        int numSplits = conf.getInt(MRJobConfig.NUM_MAPS, 1);
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (long i = 0 ; i < numSplits ; ++i){
            LongWritable key = new LongWritable();
            key.set(i);
            splits.add(new KeyInputSplit.LongKeySplit(key));
        }

        return splits;
    }
}
