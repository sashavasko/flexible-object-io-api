package org.sv.flexobject.io;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.Savable;
import org.sv.flexobject.schema.Schema;

import java.util.HashMap;
import java.util.Map;

public class GenericWriter implements Writer {
    protected static GenericWriter universalInstance = null;
    protected static Map<String, GenericWriter> instances = new HashMap<>();

    protected Schema schema = null;

    protected GenericWriter(){};

    public GenericWriter(Schema schema) {
        this.schema = schema;
    }

    /*
     * This instance does not use Schema and will rely on save() method implemented
     */
    public static GenericWriter getInstance() {
        if (universalInstance == null){
            universalInstance = new GenericWriter();
        }
        return universalInstance;
    }

    /*
     * This method will return schema specific instance and does not require save() method
     */
    public static GenericWriter getInstance(Class datumClass) {
        GenericWriter instance = instances.get(datumClass.getName());
        if (instance == null) {
            instance = new GenericWriter(Schema.getRegisteredSchema(datumClass));
            instances.put(datumClass.getName(), instance);
        }
        return instance;
    }

    @Override
    public boolean convert(Savable dbObject, OutAdapter output) throws Exception {
        if (schema != null)
            return schema.saveFields(dbObject, output);
        return dbObject.save(output);
    }
}
