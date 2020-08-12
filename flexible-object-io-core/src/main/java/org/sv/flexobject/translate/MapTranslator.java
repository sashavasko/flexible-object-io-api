package org.sv.flexobject.translate;

import java.util.Map;

public class MapTranslator implements Translator {

    Map<String,String> map;

    public MapTranslator(Map<String, String> map) {
        this.map = map;
    }

    @Override
    public String apply(String s) {
        return map.containsKey(s) ? map.get(s) : s;
    }
}
