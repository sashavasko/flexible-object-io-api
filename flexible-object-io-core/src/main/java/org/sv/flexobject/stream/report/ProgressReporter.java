package org.sv.flexobject.stream.report;

import java.util.Collection;

public interface ProgressReporter extends SizeReporter{
    default void setSize(Object optionalSizeReporter){
        if (optionalSizeReporter instanceof SizeReporter) {
            setSize(((SizeReporter) optionalSizeReporter).getSize());
        } else if (optionalSizeReporter instanceof Collection) {
            setSize(((Collection)optionalSizeReporter).size());
        } else {
            setSize(0);
        }
    }

    void setSize(long size);
    float getProgress();
    void increment(long count);

    default void increment(){
        increment(1);
    }
}
