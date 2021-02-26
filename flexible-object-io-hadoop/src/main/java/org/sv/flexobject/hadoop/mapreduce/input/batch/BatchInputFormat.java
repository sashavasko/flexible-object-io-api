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
                split.setConf(context.getConfiguration());
                split.setStartKey(startKey);
                splits.add(split);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to instantiate split", e);
            }
            startKey += conf.getSize() * batchesInSplit;
        }
        return splits;
    }

    @Override
    public RecordReader<LongWritable, VT> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            return new BatchInputConf()
                    .from(context.getConfiguration())
                    .getReader();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to instantiate record reader", e);
        }
    }
}
