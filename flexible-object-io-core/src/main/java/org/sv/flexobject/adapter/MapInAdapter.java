package org.sv.flexobject.adapter;

import org.sv.flexobject.stream.Source;

import java.util.Map;

public class MapInAdapter extends GenericInAdapter<Map> implements DynamicInAdapter{

    public MapInAdapter(Source<Map> source) {
        super(source);
    }

    @Override
    public Object get(Object fieldName) {
        return getCurrent().get(fieldName);
    }
}
