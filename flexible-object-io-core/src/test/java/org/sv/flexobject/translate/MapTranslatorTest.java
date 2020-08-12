package org.sv.flexobject.translate;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapTranslatorTest {

    @Test
    public void apply() {

        Map<String, String> map = new HashMap<>();
        map.put("foo", "notfoo");
        map.put("bar", "notbar");

        Translator translator = new MapTranslator(map);

        assertEquals("notfoo", translator.apply("foo"));
        assertEquals("notbar", translator.apply("bar"));
        assertEquals("self", translator.apply("self"));
    }
}