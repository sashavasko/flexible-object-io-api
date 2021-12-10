package org.sv.flexobject.hadoop.mapreduce.util.counter;

import org.sv.flexobject.hadoop.StreamableWritableImpl;

import java.sql.Timestamp;

public class CounterItem extends StreamableWritableImpl {
    String groupName;
    String counterName;
    Long counterValue;
    Timestamp startTimestamp;
    Timestamp commitTimestamp;
    String taskId;

    public CounterItem(String groupName, Timestamp startTimestamp, Timestamp commitTimestamp, String taskId) {
        this.groupName = groupName;
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
        this.taskId = taskId;
    }

    public void setCounterName(String counterName) {
        this.counterName = counterName;
    }

    public void setCounterValue(Long counterValue) {
        this.counterValue = counterValue;
    }
}
