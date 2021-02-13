package org.sv.flexobject.hadoop.mapreduce.util.counter;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public interface ICounter {

    Counter setContext(TaskInputOutputContext context);

    Counter setMainTitle(String... mainTitleParts);

    Counter increment();

    Counter increment(String detail);

    Counter incrementTotal(String totalName);

    Counter incrementTotal(String totalName, int value);
}
