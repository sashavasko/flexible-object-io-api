package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.sv.flexobject.hadoop.utils.IConfigured;

import java.io.IOException;

public interface MaxKeyCalculator extends IConfigured {

    Comparable calculateMaxKey() throws IOException;
}
