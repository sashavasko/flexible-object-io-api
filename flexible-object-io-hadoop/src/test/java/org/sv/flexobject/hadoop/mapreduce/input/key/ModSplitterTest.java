package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ModSplitterTest {

    Configuration rawConf = new Configuration(false);

    ModSplitter splitter = new ModSplitter();

    @Test
    public void split() {
        rawConf.setInt(MRJobConfig.NUM_MAPS, 5);

        List<InputSplit> splits = splitter.split(rawConf);

        assertEquals(5, splits.size());
        for (int i = 0 ; i < 5 ; ++i){
            LongWritable key = ((KeyInputSplit.LongKeySplit)splits.get(i)).getKey();
            assertEquals((long)i, key.get());
        }

    }
}