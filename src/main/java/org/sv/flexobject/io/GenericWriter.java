package org.sv.flexobject.io;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.Savable;

public class GenericWriter implements Writer {

    protected static GenericWriter instance = null;

    protected GenericWriter(){};

    public static GenericWriter getInstance() {
        if (instance == null)
            instance = new GenericWriter();
        return instance;
    }

    @Override
    public boolean convert(Savable dbObject, OutAdapter output) throws Exception {
        return dbObject.save(output);
    }
}
