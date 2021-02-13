package org.sv.flexobject.hadoop.mapreduce.util.counter;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.sql.Timestamp;

public class Counter implements ICounter{
    protected String groupName;
    protected String mainTitle = "";
    protected Timestamp startTimestamp = new Timestamp(System.currentTimeMillis());

    protected TaskInputOutputContext context;

    public Counter(String groupName) {
        this.groupName = groupName;
    }

    public Counter setContext(TaskInputOutputContext context) {
        this.context = context;
        return this;
    }

    public Counter setMainTitle(String... mainTitleParts) {
        mainTitle = StringUtils.join(mainTitleParts, ' ');
        return this;
    }

    public Counter increment() {
        if (context != null) {
            increment(mainTitle, 1);
        }
        return this;
    }

    public Counter increment(String detail) {
        if (context != null) {
            increment(mainTitle + ' ' + detail, 1);
        }
        return this;
    }

    public Counter incrementTotal(String totalName) {
        if (context != null) {
            increment(totalName, 1);
        }
        return this;
    }

    public Counter incrementTotal(String totalName, int value) {
        if (context != null) {
            increment(totalName, value);
        }
        return this;
    }

    protected void increment (String name, int value){
        context.getCounter(groupName, name).increment(value);
    }
}
