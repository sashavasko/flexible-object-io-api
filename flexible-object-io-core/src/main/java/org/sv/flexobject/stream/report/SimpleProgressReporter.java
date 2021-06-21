package org.sv.flexobject.stream.report;

public class SimpleProgressReporter implements ProgressReporter{

    long size = 0;
    long sizeEstimated;
    long current = 0;

    public void setSize(long size){
        if (size <= 0){
            this.size = 0 ;
            sizeEstimated = 1;
        } else {
            this.size = size ;
            sizeEstimated = size;
        }
    }

    @Override
    public float getProgress() {
        if ((size == 0 && current + current / 4  > sizeEstimated)
            || current > sizeEstimated){
            sizeEstimated += (sizeEstimated+1)/2l;
        }
        return (float)current / (float) sizeEstimated;
    }

    @Override
    public void increment(long count) {
        current += count;
    }
}
