package org.sv.flexobject.schema;

import java.util.HashMap;
import java.util.Map;

public class SchemaRegistry {

    private static final SchemaRegistry instance = new SchemaRegistry();
    private final Map<String, Map<String, Integer>> paramNamesXrefs = new HashMap<>();

    private final Map<String, Schema> schemas = new HashMap<>();

    private SchemaRegistry(){};

    public static SchemaRegistry getInstance(){
        return instance;
    }

    public Map<String, Integer> getParamNamesXref(String name){
        checkClassLoaded(name);
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
        if (!checkClassLoaded(name)) return null;
        return schemas.get(name);
    }

    public boolean hasSchema(String name){
        return schemas.containsKey(name);
    }

    public void clear(){
        schemas.clear();
        paramNamesXrefs.clear();
    }

    private boolean checkClassLoaded(String name) {
        if (!schemas.containsKey(name)){
            try{
                Class<?> clazz = Class.forName(name);
                clazz.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                return false;
            }
        }
        return true;
    }
}
