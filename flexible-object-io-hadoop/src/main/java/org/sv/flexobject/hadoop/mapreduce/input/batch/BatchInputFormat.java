package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchInputFormat<VT> extends InputFormat<LongWritable, VT> {

    public static class Long extends BatchInputFormat<LongWritable>{}
    public static class Text extends BatchInputFormat<org.apache.hadoop.io.Text>{}
    public static class Double extends BatchInputFormat<DoubleWritable>{}

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        BatchInputConf conf = new BatchInputConf().from(context.getConfiguration());

        long splitsCount = (conf.batchesNum + conf.batchesPerSplit - 1)/conf.batchesPerSplit;
        long maxControlNumber = conf.keyStart + conf.batchesNum * conf.size;

        List<InputSplit> splits = new ArrayList<>();
        long startKey = conf.keyStart;
        for (int i = 0 ; i < splitsCount ; i++){
            long batchesInSplit = conf.batchesPerSplit;
            if (maxControlNumber - startKey < conf.batchesPerSplit)
                batchesInSplit = maxControlNumber - startKey;

            try {
                BatchInputSplit split = conf.splitClass.newInstance();
                split.setConf(context.getConfiguration());
                split.setStartKey(startKey);
                splits.add(split);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to instantiate split", e);
            }
            startKey += conf.size * batchesInSplit;
        }
        return splits;
    }

    @Override
    public RecordReader<LongWritable, VT> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            return new BatchInputConf()
                    .from(context.getConfiguration())
                    .readerClass
                    .newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to instantiate record reader", e);
        }
    }
}
