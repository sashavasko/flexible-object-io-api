package org.sv.flexobject.hadoop.mapreduce.input.split.timerange;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;

import java.io.IOException;

public class TimeRangeRecordReader extends RecordReader<LongWritable, LongWritable> {
    long currentStartTime;
    long totalRange;

    TimeRangeSplit split;

    LongWritable key = new LongWritable();
    LongWritable value = new LongWritable();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        split = ((ProxyInputSplit)inputSplit).getData();

        currentStartTime = split.getStartTimeMillis();
        totalRange = split.getEndTimeMillis() - split.getStartTimeMillis();;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (currentStartTime >= split.getEndTimeMillis())
            return false;
        key.set(currentStartTime);

        currentStartTime += split.getMillisPerRecord();
        if (currentStartTime > split.getEndTimeMillis())
            value.set(split.getEndTimeMillis());
        else
            value.set(currentStartTime);

        return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public LongWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (totalRange - (split.getEndTimeMillis() - currentStartTime))/(float)totalRange;
    }

    @Override
    public void close() throws IOException {
    }
}
