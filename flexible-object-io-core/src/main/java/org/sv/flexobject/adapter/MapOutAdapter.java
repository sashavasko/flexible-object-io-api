package org.sv.flexobject.adapter;

import org.sv.flexobject.stream.Sink;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MapOutAdapter extends GenericOutAdapter<Map> implements DynamicOutAdapter {

    public enum PARAMS {
        namespace
    }

    protected String namespace = "";

    public MapOutAdapter() {
        super(()->new HashMap<String, Object>());
    }

    public MapOutAdapter(Sink sink) {
        super(sink, ()->new HashMap<String, Object>());
    }

    public MapOutAdapter(Supplier<Map> recordFactory) {
        super(recordFactory);
    }

    public MapOutAdapter(Sink sink, Supplier<Map> recordFactory) {
        super(sink, recordFactory);
    }

    public MapOutAdapter(Class<? extends Map> recordClass) {
        super(recordClass);
    }

    public MapOutAdapter(Sink sink, Class<? extends Map> recordClass) {
        super(sink, recordClass);
    }

    @Override
    public String translateOutputFieldName(String fieldName) {
        return namespace.isEmpty()? fieldName : namespace + "." + fieldName;
    }


    @Override
    public Object put(String fieldName, Object value) throws Exception{
        return getCurrent().put(translateOutputFieldName(fieldName), value);
    }

    public void setParam(String key, Object value){
        try{
            setParam(PARAMS.valueOf(key), value);
        }catch (IllegalArgumentException e){
            super.setParam(key, value);
        }
    }

    public void setParam(PARAMS key, Object value){
        if (PARAMS.namespace == key && value != null && value instanceof String)
            namespace = (String) value;
    }
}
