package org.sv.flexobject.io;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.Savable;

public interface Writer {

    default boolean convert(Savable datum, OutAdapter output) throws Exception{
        return datum.save(output);
    }
}
