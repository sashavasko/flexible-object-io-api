package org.sv.flexobject.util;

import java.util.Map;

public class MapBuilder {

    Map map;

    private MapBuilder(Class <? extends Map> mapClass) {
        this.map = InstanceFactory.get(mapClass);
    }

    public static MapBuilder forClass(Class <? extends Map> mapClass){
        MapBuilder builder = new MapBuilder(mapClass);
        return builder;
    }

    public MapBuilder put(Object key, Object value){
        map.put(key, value);
        return this;
    }

    public MapBuilder putAll(Map other){
        map.putAll(other);
        return this;
    }

    public Map build(){
        return map;
    }
}
