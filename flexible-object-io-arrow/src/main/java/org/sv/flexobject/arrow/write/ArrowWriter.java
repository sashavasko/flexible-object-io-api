package org.sv.flexobject.arrow.write;

public interface ArrowWriter extends AutoCloseable{
    void write(int rowIndex, Object datum);

    void setNull(int rowIndex);

    void commit();

    void newBatch();
}
