package org.sv.flexobject.adapter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.translate.NamespaceTranslator;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MapInAdapterTest {

    Map<String, Object> propertiesMap = new HashMap<>();
    MapInAdapter adapter;

    public static class ConfigPOJO extends StreamableWithSchema {
        Boolean booleanValue;
        String stringValue;
        Integer nullValue;

        @Override
        public String toString() {
            return "ConfigPOJO{" +
                    "booleanValue=" + booleanValue +
                    ", stringValue='" + stringValue + '\'' +
                    ", nullValue=" + nullValue +
                    '}';
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        adapter = (MapInAdapter) GenericInAdapter.build(MapInAdapter.class, new SingleValueSource(propertiesMap));
    }

    @AfterEach
    public void tearDown() throws Exception {
        propertiesMap.clear();
    }

    @Test
    public void get() throws Exception {
        propertiesMap.put("foo", 12345);
        propertiesMap.put("bar", "bozo");

        adapter.next();

        assertEquals(12345, adapter.get("foo"));
        assertEquals("bozo", adapter.get("bar"));
        assertNull(adapter.get("missing"));
    }

    @Test
    public void setParam() throws Exception {
        assertEquals("foo", adapter.translateInputFieldName("foo"));
        assertEquals("bar", adapter.translateInputFieldName("bar"));

        adapter.setParam(GenericInAdapter.PARAMS.fieldNameTranslator, new NamespaceTranslator("sushi"));

        assertEquals("sushi.foo", adapter.translateInputFieldName("foo"));
        assertEquals("sushi.bar", adapter.translateInputFieldName("bar"));
    }

    @Test
    public void toPOJO() throws Exception {
        adapter.setParam(GenericInAdapter.PARAMS.fieldNameTranslator, new NamespaceTranslator("config.pojo"));

        propertiesMap.put("config.pojo.booleanValue", true);
        propertiesMap.put("config.pojo.stringValue", "bozo");

        ConfigPOJO configPOJO = new ConfigPOJO();

        adapter.consume(configPOJO::load);

        System.out.println(configPOJO);
        assertTrue(configPOJO.booleanValue);
        assertEquals("bozo", configPOJO.stringValue);
        assertNull(configPOJO.nullValue);
    }
}