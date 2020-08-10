package org.sv.flexobject.io;

public interface CloseableConsumer extends Consumer, AutoCloseable{

    @Override
    void close() throws Exception;

    @Override
    default void cleanup() throws Exception{
        close();
    }
}
