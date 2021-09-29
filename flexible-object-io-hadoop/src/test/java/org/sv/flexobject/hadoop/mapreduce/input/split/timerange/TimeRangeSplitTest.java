package org.sv.flexobject.hadoop.mapreduce.input.split.timerange;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TimeRangeSplitTest {

    @Test
    public void secondsToMillis() {
        TimeRangeSplit split = new TimeRangeSplit(1, 5, 2);

        assertEquals(1000l, split.getStartTimeMillis());
        assertEquals(5000l, split.getEndTimeMillis());
        assertEquals(2000l, split.getMillisPerRecord());
    }

    @Test
    public void exactMillisPerRecord() throws IOException, InterruptedException {
        TimeRangeSplit split = new TimeRangeSplit(1000l, 2000l, 500l);

        assertEquals(500l, split.getMillisPerRecord());
        assertEquals(2l, split.getLength());
    }

    @Test
    public void roundUpLength() throws IOException, InterruptedException {
        TimeRangeSplit split = new TimeRangeSplit(1000l, 2000l, 600l);

        assertEquals(600l, split.getMillisPerRecord());
        assertEquals(2l, split.getLength());
    }

    @Test
    public void tooMuchMillisPerRecord() throws IOException, InterruptedException {
        TimeRangeSplit split = new TimeRangeSplit(1000l, 2000l, 2000l);

        assertEquals(1000l, split.getMillisPerRecord());
        assertEquals(1l, split.getLength());
    }
}