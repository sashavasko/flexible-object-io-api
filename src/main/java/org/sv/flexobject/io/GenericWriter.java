package org.sv.flexobject.io;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.Savable;
import org.sv.flexobject.schema.Schema;

public class GenericWriter implements Writer {

    protected static GenericWriter instance = null;

    protected Schema schema;

    protected GenericWriter(){};

    public GenericWriter(Schema schema) {
        this.schema = schema;
    }

    public static GenericWriter getInstance() {
        if (instance == null)
            instance = new GenericWriter();
        return instance;
    }

    @Override
    public boolean convert(Savable dbObject, OutAdapter output) throws Exception {
        if (schema != null)
            return schema.saveFields(dbObject, output);
        return dbObject.save(output);
    }
}
