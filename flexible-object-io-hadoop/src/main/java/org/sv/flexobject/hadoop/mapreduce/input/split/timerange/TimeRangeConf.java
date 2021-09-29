package org.sv.flexobject.hadoop.mapreduce.input.split.timerange;


import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.properties.Namespace;

import java.sql.Timestamp;

public class TimeRangeConf<SELF extends InputConf> extends InputConf<SELF> {
    public static final String SUBNAMESPACE = "time.range";

    public Timestamp startdate;
    public Timestamp enddate;
    public Long millisPerRecord;

    public TimeRangeConf() {
        super(SUBNAMESPACE);
    }

    public TimeRangeConf(String child) {
        super(makeMyNamespace(getParentNamespace(InputConf.class), SUBNAMESPACE), child);
    }

    public TimeRangeConf(Namespace parent) {
        super(parent, SUBNAMESPACE);
    }

    public TimeRangeConf(Namespace parent, String child) {
        super(parent, child);
    }

    @Override
    public SELF setDefaults() {
        super.setDefaults();
        enddate = new Timestamp(System.currentTimeMillis());
        millisPerRecord = 60l*60l*1000l;

        readerClass = TimeRangeRecordReader.class;
        splitterClass = TimeRangeSplitter.class;

        return (SELF)this;
    }

    // Can be overridden in extending classes to add logic for time period validation
    public void validate(){}

    public int getSecondsPerSplit() {
        validate();
        return (int) (millisPerRecord /1000);
    }

    public SELF setSecondsPerRecord(long secondsPerRecord){
        millisPerRecord = secondsPerRecord*1000;
        return (SELF)this;
    }

    public long getMillisPerRecord() {
        validate();
        return millisPerRecord;
    }

    public SELF setMillisPerRecord(long millisPerRecord){
        this.millisPerRecord = millisPerRecord;
        return (SELF)this;
    }

    public int getStartSeconds(){
        validate();
        return (int) (startdate.getTime()/1000);
    }

    public SELF setStartSecond(long startSeconds){
        startdate = new Timestamp(startSeconds*1000);
        return (SELF)this;
    }

    public int getEndSeconds(){
        validate();
        return (int) (enddate.getTime()/1000);
    }

    public SELF setEndSecond(long endSeconds){
        enddate = new Timestamp(endSeconds*1000);
        return (SELF)this;
    }

    public Timestamp getStart(){
        validate();
        return startdate;
    }

    public SELF setStart(Timestamp start){
        startdate = start;
        return (SELF)this;
    }

    public Timestamp getEnd(){
        validate();
        return enddate;
    }

    public SELF setEnd(Timestamp end){
        enddate = end;
        return (SELF)this;
    }
}
