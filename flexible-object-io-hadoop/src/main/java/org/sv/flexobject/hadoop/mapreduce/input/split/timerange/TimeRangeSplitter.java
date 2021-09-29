package org.sv.flexobject.hadoop.mapreduce.input.split.timerange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.mapreduce.input.Splitter;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mapreduce.MRJobConfig.JOB_RUNNING_MAP_LIMIT;

public class TimeRangeSplitter extends Configured implements Splitter {
    public static final int DEFAULT_TIME_PERIODS = 100;
    TimeRangeConf conf = InstanceFactory.get(TimeRangeConf.class);

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if(conf != null)
            this.conf.from(conf);
    }

    public TimeRangeConf getTimeRangeConf() {
        return conf;
    }

    public int calculateTimePeriods(){
        return getConf().getInt(JOB_RUNNING_MAP_LIMIT, DEFAULT_TIME_PERIODS);
    }

    @Override
    public List<InputSplit> split(Configuration rawConf) throws IOException {
        setConf(rawConf);

        long startTime = conf.getStart().getTime();
        long endTime = conf.getEnd().getTime();

        List<InputSplit> splits = new ArrayList<InputSplit>();
        int timePeriods = calculateTimePeriods();
        long timeIncrementMillis = (endTime - startTime + timePeriods - 1) / timePeriods;
        if (timeIncrementMillis == 0)
            timeIncrementMillis = 1;
        while (startTime < endTime) {
            long nextStartTime = startTime + timeIncrementMillis;
            if (nextStartTime > endTime)
                nextStartTime = endTime;
            TimeRangeSplit split = new TimeRangeSplit(startTime, nextStartTime, conf.getMillisPerRecord());
            splits.add(new ProxyInputSplit(split));
            startTime = nextStartTime;
        }

        return splits;
    }
}
