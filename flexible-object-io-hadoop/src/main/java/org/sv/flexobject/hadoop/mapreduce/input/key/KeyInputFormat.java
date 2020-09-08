package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

public class KeyInputFormat<KT,VT> extends InputFormat<KT, VT> {

    public static class LongLong extends KeyInputFormat<LongWritable,LongWritable> {}
    public static class LongText extends KeyInputFormat<LongWritable, Text> {}
    public static class LongDouble extends KeyInputFormat<LongWritable,DoubleWritable> {}
    public static class TextLong extends KeyInputFormat<Text,LongWritable> {}
    public static class TextText extends KeyInputFormat<Text, Text> {}
    public static class TextDouble extends KeyInputFormat<Text,DoubleWritable> {}

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        KeyInputConf conf = new KeyInputConf().from(context.getConfiguration());
        try {
            return conf.splitterClass
                    .newInstance()
                    .split(context.getConfiguration());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to instantiate splitter " + conf.splitterClass.getName(), e);
        }
    }

    @Override
    public RecordReader<KT, VT> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            return new KeyInputConf()
                    .from(context.getConfiguration())
                    .readerClass
                    .newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to instantiate record reader", e);
        }
    }
}
