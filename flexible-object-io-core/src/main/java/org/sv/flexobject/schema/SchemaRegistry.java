package org.sv.flexobject.schema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SchemaRegistry {

    private static final SchemaRegistry instance = new SchemaRegistry();
    private final Map<String, Map<String, Integer>> paramNamesXrefs = new HashMap<>();
    private final Set<String> loadedClasses = new HashSet<>();

    private final Map<String, AbstractSchema> schemas = new HashMap<>();

    private SchemaRegistry(){};

    public static SchemaRegistry getInstance(){
        return instance;
    }

    public Map<String, Integer> getParamNamesXref(String name){
        return getParamNamesXref(name, Schema.class);
    }

    public Map<String, Integer> getParamNamesXref(String name, Class<? extends AbstractSchema> schemaClass){
        checkClassLoaded(name);
        String key = makeKey(name, schemaClass);
        return paramNamesXrefs.get(key);
    }

    public void setParamNamesXref(String name, Map<String, Integer> xref){
        setParamNamesXref(name, Schema.class, xref);
    }

    public void setParamNamesXref(String name, Class<? extends AbstractSchema> schemaClass, Map<String, Integer> xref){
        String key = makeKey(name, schemaClass);
        paramNamesXrefs.put(key, xref);
    }

    public static String makeKey(String name, Class<? extends AbstractSchema> schemaClass){
        return schemaClass.getSimpleName() + "::" + name;
    }

    public void registerSchema(AbstractSchema schema){
        String key = makeKey(schema.getName(), schema.getClass());
        if (!schemas.containsKey(key)){
            schemas.put(key, schema);
            setParamNamesXref(schema.getName(), schema.getParamNamesXref());
        }
    }

    public <T extends AbstractSchema> T getSchema(String name, Class<? extends AbstractSchema> schemaClass){
        if (!checkClassLoaded(name)) return null;
        return (T)schemaClass.cast(schemas.get(makeKey(name, schemaClass)));
    }

    public Schema getSchema(String name){
        return getSchema(name, Schema.class);
    }

    public boolean hasSchema(String name, Class<? extends AbstractSchema> schemaClass){
        if (!checkClassLoaded(name)) return false;
        return schemas.containsKey(makeKey(name, schemaClass));
    }

    public boolean hasSchema(String name){
        return hasSchema(name, Schema.class);
    }

    public void clear(){
        schemas.clear();
        paramNamesXrefs.clear();
        loadedClasses.clear();
    }

    private boolean checkClassLoaded(String name) {
        if (!schemas.containsKey(name)){
            if (!loadedClasses.contains(name)) {
                try {
                    Class<?> clazz = Class.forName(name);
                    loadedClasses.add(name);
                    clazz.newInstance();
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    return false;
                }
            }
        }
        return true;
    }
}
