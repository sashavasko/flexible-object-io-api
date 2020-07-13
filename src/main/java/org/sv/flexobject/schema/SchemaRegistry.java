package org.sv.flexobject.schema;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistry {

    private static final SchemaRegistry instance = new SchemaRegistry();
    private static final Map<String, Map<String, Integer>> paramNamesXrefs = new HashMap<>();

    private static final Map<String, Schema> schemas = new HashMap<>();

    private SchemaRegistry(){};

    public static SchemaRegistry getInstance(){
        return instance;
    }

    public Map<String, Integer> getParamNamesXref(String name){
        return paramNamesXrefs.get(name);
    }

    public void setParamNamesXref(String name, Map<String, Integer> xref){
        paramNamesXrefs.put(name, xref);
    }

    public void registerSchema(Schema schema){
        if (!schemas.containsKey(schema.getName())){
            schemas.put(schema.getName(), schema);
            setParamNamesXref(schema.getName(), schema.getParamNamesXref());
        }
    }

    public Schema getSchema(String name){
        return schemas.get(name);
    }
}
