package org.sv.flexobject.io;

import org.sv.flexobject.Savable;

public interface Consumer {

    boolean consume(Savable datum) throws Exception;
    long getRecordsConsumed();

    void cleanup() throws Exception;

}
