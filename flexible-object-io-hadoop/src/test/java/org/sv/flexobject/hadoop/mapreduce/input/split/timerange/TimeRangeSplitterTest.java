package org.sv.flexobject.hadoop.mapreduce.input.split.timerange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.schema.DataTypes;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimeRangeSplitterTest {

    @Test
    public void oneSplit() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("sv.input.time.range.startdate", "2020-01-01 11:00:00");
        conf.set("sv.input.time.range.enddate", "2020-01-01 11:00:01");
        conf.setLong("sv.input.time.range.millis.per.record", 500);
        conf.setInt("mapreduce.job.running.map.limit", 1);

        TimeRangeSplitter splitter = new TimeRangeSplitter();
        List<InputSplit> splits = splitter.split(conf);

        assertEquals(1, splits.size());
        assertTrue(splits.get(0) instanceof ProxyInputSplit);
        TimeRangeSplit split = ((ProxyInputSplit)splits.get(0)).getData();
        assertEquals(DataTypes.timestampConverter("2020-01-01 11:00:00").getTime(), split.getStartTimeMillis());
        assertEquals(DataTypes.timestampConverter("2020-01-01 11:00:01").getTime(), split.getEndTimeMillis());
        assertEquals(500l, split.getMillisPerRecord());
    }

    @Test
    public void threeSplits() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("sv.input.time.range.startdate", "2020-01-01 11:00:00");
        conf.set("sv.input.time.range.enddate", "2020-01-01 11:00:01");
        conf.setLong("sv.input.time.range.millis.per.record", 400);
        conf.setInt("mapreduce.job.running.map.limit", 3);

        TimeRangeSplitter splitter = new TimeRangeSplitter();
        List<InputSplit> splits = splitter.split(conf);

        assertEquals(3, splits.size());
        assertTrue(splits.get(0) instanceof ProxyInputSplit);
        assertTrue(splits.get(1) instanceof ProxyInputSplit);
        assertTrue(splits.get(2) instanceof ProxyInputSplit);

        TimeRangeSplit split0 = ((ProxyInputSplit)splits.get(0)).getData();
        TimeRangeSplit split1 = ((ProxyInputSplit)splits.get(1)).getData();
        TimeRangeSplit split2 = ((ProxyInputSplit)splits.get(2)).getData();
        assertEquals(DataTypes.timestampConverter("2020-01-01 11:00:00.0").getTime(), split0.getStartTimeMillis());
        assertEquals(DataTypes.timestampConverter("2020-01-01 11:00:00.334").getTime(), split0.getEndTimeMillis());
        assertEquals(334l, split0.getMillisPerRecord());
        assertEquals(DataTypes.timestampConverter("2020-01-01 11:00:00.334").getTime(), split1.getStartTimeMillis());
        assertEquals(DataTypes.timestampConverter("2020-01-01 11:00:00.668").getTime(), split1.getEndTimeMillis());
        assertEquals(334l, split1.getMillisPerRecord());
        assertEquals(DataTypes.timestampConverter("2020-01-01 11:00:00.668").getTime(), split2.getStartTimeMillis());
        assertEquals(DataTypes.timestampConverter("2020-01-01 11:00:01").getTime(), split2.getEndTimeMillis());
        assertEquals(332, split2.getMillisPerRecord());
    }
}