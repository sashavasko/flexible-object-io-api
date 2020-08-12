package org.sv.flexobject.adapter;

import java.util.Map;

public class MapInAdapter extends GenericInAdapter<Map> implements DynamicInAdapter{

    public enum PARAMS {
        namespace
    }

    protected String namespace = "";

    @Override
    public String translateInputFieldName(String fieldName) {
        return namespace.isEmpty()? fieldName : namespace + "." + fieldName;
    }

    @Override
    public Object get(Object fieldName) {
        return getCurrent().get(translateInputFieldName((String) fieldName));
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
