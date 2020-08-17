package org.sv.flexobject.properties;

import org.junit.Before;
import org.junit.Test;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.translate.NamespaceTranslator;
import org.sv.flexobject.translate.UnderscoreTranslator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class PropertiesWrapperTest {

    PropertiesWrapper testProps;

    public static class TestProps extends PropertiesWrapper<TestProps> {
        public Integer intProp;
        public String stringProp;
        public Double doubleProp;
        public Boolean boolProp;
        public Boolean nullProp;

    }

    @Before
    public void setUp() throws Exception {
        testProps = new TestProps();
    }

    @Test
    public void getProps() throws Exception {
        testProps.set("intProp", 12345);
        testProps.set("stringProp", "foobar");
        testProps.set("doubleProp", 12.789);
        testProps.set("boolProp", false);

        Properties props = testProps.getProps();

        assertEquals(12345, props.get("intProp"));
        assertEquals("foobar", props.get("stringProp"));
        assertEquals(12.789, props.get("doubleProp"));
        assertFalse((Boolean) props.get("boolProp"));
        assertNull(props.get("nullProp"));

    }

    @Test
    public void getMap() throws Exception {
        testProps.set("intProp", 123);
        testProps.set("stringProp", "barfoo");
        testProps.set("doubleProp", 12.7);
        testProps.set("boolProp", true);

        Map map = testProps.getMap();

        assertEquals(123, map.get("intProp"));
        assertEquals("barfoo", map.get("stringProp"));
        assertEquals(12.7, map.get("doubleProp"));
        assertTrue((Boolean) map.get("boolProp"));
        assertFalse(map.containsKey("nullProp"));
    }

    @Test
    public void testGetMap() throws Exception {
        Map map = testProps.getMap(LinkedHashMap.class);

        assertTrue(map instanceof LinkedHashMap);
    }

    @Test
    public void from() throws Exception {
        Map map = new HashMap();
        map.put("intProp", 789);
        map.put("stringProp", "foo");

        TestProps propsObject = new TestProps().from(map);

        assertEquals(789, (int)propsObject.intProp);
        assertEquals("foo", propsObject.stringProp);

        Properties properties = new Properties();
        properties.put("doubleProp", 1.456);
        properties.put("stringProp", "bar");

        propsObject.from(properties);

        assertEquals("bar", propsObject.stringProp);
        assertEquals(1.456, propsObject.doubleProp, 0.001);
        assertNull(propsObject.nullProp);
    }

    @Test
    public void fromWithNamespace() throws Exception {
        Map map = new HashMap();
        map.put("foo.bar.intProp", 789);
        map.put("foo.bar.stringProp", "foo");
        map.put("foo.barbar.doubleProp", 3.678);
        map.put("foofoo.bar.boolProp", true);

        TestProps propsObject = new TestProps().from(map, "foo.bar");

        assertEquals(789, (int)propsObject.intProp);
        assertEquals("foo", propsObject.stringProp);
        assertNull(propsObject.doubleProp);
        assertNull(propsObject.boolProp);
    }

    @Test
    public void fromWithTranslator() throws Exception {
        Map map = new HashMap();
        map.put("foo.bar.int_prop", 789);
        map.put("foo.bar.string_prop", "foo");
        map.put("foo.barbar.double_prop", 3.678);
        map.put("foofoo.bar.bool_prop", true);

        TestProps propsObject = new TestProps().from(map, new UnderscoreTranslator().andThen(new NamespaceTranslator("foo.bar")));

        assertEquals(789, (int)propsObject.intProp);
        assertEquals("foo", propsObject.stringProp);
        assertNull(propsObject.doubleProp);
        assertNull(propsObject.boolProp);
    }

    @Test
    public void fromAdapter() throws Exception {
        InAdapter adapter = JsonInputAdapter.forValue("{'intProp':12345,'stringProp':'foo','doubleProp':12.78,'boolProp':true}".replaceAll("'", "\""));

        TestProps propsObject = new TestProps().from(adapter);

        assertEquals(12345, (int)propsObject.intProp);
        assertEquals("foo", propsObject.stringProp);
        assertEquals(12.78, propsObject.doubleProp, 0.01);
        assertTrue(propsObject.boolProp);
    }
}