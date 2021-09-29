package org.sv.flexobject.hadoop.mapreduce.input.split.timerange;

import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;

import java.io.IOException;

import static org.junit.Assert.*;

public class TimeRangeRecordReaderTest {
    @Test
    public void readOneRecord() throws IOException, InterruptedException {
        TimeRangeRecordReader reader = new TimeRangeRecordReader();
        TimeRangeSplit split = new TimeRangeSplit(1000l, 2000l, 1000l);


        reader.initialize(new ProxyInputSplit(split), null);

        assertTrue(reader.nextKeyValue());

        LongWritable key = reader.getCurrentKey();
        LongWritable value = reader.getCurrentValue();

        assertEquals(1000l, key.get());
        assertEquals(2000l, value.get());

        assertFalse(reader.nextKeyValue());

    }

    @Test
    public void readTreeRecord() throws IOException, InterruptedException {
        TimeRangeRecordReader reader = new TimeRangeRecordReader();
        TimeRangeSplit split = new TimeRangeSplit(1000l, 2000l, 334l);

        reader.initialize(new ProxyInputSplit(split), null);

        assertTrue(reader.nextKeyValue());

        LongWritable key = reader.getCurrentKey();
        LongWritable value = reader.getCurrentValue();

        assertEquals(1000l, key.get());
        assertEquals(1334l, value.get());
        assertEquals(0.33, reader.getProgress(), 0.01);

        assertTrue(reader.nextKeyValue());

        key = reader.getCurrentKey();
        value = reader.getCurrentValue();

        assertEquals(1334l, key.get());
        assertEquals(1668l, value.get());
        assertEquals(0.66, reader.getProgress(), 0.01);

        assertTrue(reader.nextKeyValue());

        key = reader.getCurrentKey();
        value = reader.getCurrentValue();

        assertEquals(1668l, key.get());
        assertEquals(2000l, value.get());
        assertEquals(1f, reader.getProgress(), 0.01);

        assertFalse(reader.nextKeyValue());
    }

}