package org.sv.flexobject.hadoop.mapreduce.input.split.timerange;


import org.sv.flexobject.hadoop.StreamableWritableImpl;
import org.sv.flexobject.hadoop.mapreduce.input.split.InputSplitImpl;

import java.io.IOException;

public class TimeRangeSplit extends StreamableWritableImpl implements InputSplitImpl {
    long startTimeMillis;
    long endTimeMillis;
    long millisPerRecord;

    public TimeRangeSplit() {
    }

    public TimeRangeSplit(int startTimeSeconds, int endTimeSeconds, int secondsPerSplit) {
        this(startTimeSeconds*1000l, endTimeSeconds*1000l, secondsPerSplit*1000l);
    }

    public TimeRangeSplit(long startTimeMillis, long endTimeMillis, long millisPerRecord) {
        this.startTimeMillis = startTimeMillis;
        this.endTimeMillis = endTimeMillis;

        this.millisPerRecord = millisPerRecord;

        if (millisPerRecord > endTimeMillis - startTimeMillis)
            this.millisPerRecord = endTimeMillis - startTimeMillis;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public long getEndTimeMillis() {
        return endTimeMillis;
    }

    public long getMillisPerRecord() {
        return millisPerRecord;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return (endTimeMillis-startTimeMillis + millisPerRecord - 1)/ millisPerRecord;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }
}
