package org.sv.flexobject.copy;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.adapter.DynamicInAdapter;
import org.sv.flexobject.adapter.DynamicOutAdapter;

import java.util.HashMap;
import java.util.Set;

public class CopyAdapter extends HashMap<String, Object> implements DynamicInAdapter, DynamicOutAdapter, Copyable {

    public enum PARAMS {
        allowedInputFields,
        allowedOutputFields
    }

    protected Set<String> allowedInputFields = null;
    protected Set<String> allowedOutputFields = null;


    public void setAllowedInputFields(Set<String> allowedInputFields) {
        this.allowedInputFields = allowedInputFields;
    }

    public void setAllowedOutputFields(Set<String> allowedOutputFields) {
        this.allowedOutputFields = allowedOutputFields;
    }

    @Override
    public Object put(String translatedFieldName, Object value) {
        String fieldName = translateOutputFieldName(translatedFieldName);
        if (allowedOutputFields == null || allowedOutputFields.contains(fieldName))
            return super.put(fieldName, value);
        return null;
    }

    @Override
    public Object get(Object translatedFieldName) {
        String fieldName = translateInputFieldName((String) translatedFieldName);
        if (allowedInputFields == null || allowedInputFields.contains(fieldName))
            return super.get(fieldName);
        return null;
    }

    @Override
    public boolean next() throws Exception {
        return true;
    }

    @Override
    public OutAdapter save() throws Exception {
        return this;
    }

    @Override
    public boolean shouldSave() {
        return true;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void copyRecord(CopyAdapter to) throws Exception {
        to.putAll(this);
    }

    @Override
    public void setParam(String key, Object value) {
        setParam(PARAMS.valueOf(key), value);
    }

    public void setParam(PARAMS key, Object value) {
        if (PARAMS.allowedInputFields == key && value != null && value instanceof Set)
            allowedInputFields = (Set<String>) value;
        else if (PARAMS.allowedOutputFields == key && value != null && value instanceof Set)
            allowedOutputFields = (Set<String>) value;
    }
}
