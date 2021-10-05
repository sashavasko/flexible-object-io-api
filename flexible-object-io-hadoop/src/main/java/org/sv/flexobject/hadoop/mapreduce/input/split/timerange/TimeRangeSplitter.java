package org.sv.flexobject.hadoop.mapreduce.input.split.timerange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.hadoop.mapreduce.input.InputConfOwner;
import org.sv.flexobject.hadoop.mapreduce.input.Splitter;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mapreduce.MRJobConfig.JOB_RUNNING_MAP_LIMIT;

public class TimeRangeSplitter implements Splitter, InputConfOwner {
    public static final int DEFAULT_TIME_PERIODS = 100;
    TimeRangeConf conf;

    public TimeRangeConf getTimeRangeConf() {
        return conf;
    }

    public int calculateTimePeriods(Configuration rawConf){
        return rawConf.getInt(JOB_RUNNING_MAP_LIMIT, DEFAULT_TIME_PERIODS);
    }

    @Override
    public List<InputSplit> split(Configuration rawConf) throws IOException {
        if (conf == null) {
            conf = InstanceFactory.get(TimeRangeConf.class);
            conf.from(rawConf);
        }

        conf.validate();

        long startTime = conf.getStart().getTime();
        long endTime = conf.getEnd().getTime();

        List<InputSplit> splits = new ArrayList<>();

        int timePeriods = calculateTimePeriods(rawConf);
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

    @Override
    public void setInputConf(InputConf conf) {
        this.conf = (TimeRangeConf) conf;
    }

    @Override
    public InputConf getInputConf() {
        return conf;
    }
}
