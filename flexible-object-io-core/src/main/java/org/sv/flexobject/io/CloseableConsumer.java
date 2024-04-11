package org.sv.flexobject.io;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Closeable consumer can be used in try ().
 * When it is closed  - if there were any errors during
 * consumption - it will throw the last one.
 * To get all errors getAllErrors() method must be called in a
 * catch block.
 *
 */
public abstract class CloseableConsumer extends Consumer implements AutoCloseable{

    @Override
    public void close() throws Exception {
        List<Exception> exceptions = getAllErrors();
        if (!exceptions.isEmpty()){
            throw exceptions.get(exceptions.size()-1);
        }
    }
}
