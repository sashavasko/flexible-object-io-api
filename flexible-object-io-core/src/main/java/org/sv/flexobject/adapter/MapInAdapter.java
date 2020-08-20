package org.sv.flexobject.adapter;

import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.sources.SingleValueSource;

import java.util.Map;

public class MapInAdapter extends GenericInAdapter<Map> implements DynamicInAdapter{

    public MapInAdapter(Source<Map> source) {
        super(source);
    }

    @Override
    public Object get(Object translatedFieldName) {
        return getCurrent().get(translatedFieldName);
    }

    public static MapInAdapter forValue(Map map) throws Exception {
        if (map != null) {
            SingleValueSource<Map> source = new SingleValueSource<>(map);
            return new MapInAdapter(source);
        }
        return null;
    }
}
