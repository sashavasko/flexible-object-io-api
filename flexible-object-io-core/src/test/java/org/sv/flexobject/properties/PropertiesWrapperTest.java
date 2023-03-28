package org.sv.flexobject.properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
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


        @Override
        public TestProps setDefaults() {
            return this;
        }
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

        assertEquals(12345, props.get("int.prop"));
        assertEquals("foobar", props.get("string.prop"));
        assertEquals(12.789, props.get("double.prop"));
        assertFalse((Boolean) props.get("bool.prop"));
        assertNull(props.get("null.prop"));

    }

    @Test
    public void getMap() throws Exception {
        testProps.set("intProp", 123);
        testProps.set("stringProp", "barfoo");
        testProps.set("doubleProp", 12.7);
        testProps.set("boolProp", true);

        Map map = testProps.getMap();

        assertEquals(123, map.get("int.prop"));
        assertEquals("barfoo", map.get("string.prop"));
        assertEquals(12.7, map.get("double.prop"));
        assertTrue((Boolean) map.get("bool.prop"));
        assertFalse(map.containsKey("null.prop"));
    }

    @Test
    public void testGetMap() throws Exception {
        Map map = testProps.getMap(LinkedHashMap.class);

        assertTrue(map instanceof LinkedHashMap);
    }

    @Test
    public void fromCli() throws Exception {
        Options options = new Options();
        options.addOption("i", "intProp", true, "key");
        options.addOption("s", "stringProp", true, "level");

        CommandLine cli = new DefaultParser().parse(options, new String[]{"-i", "10", "-s", "foobar"}, true);

        TestProps propsObject = new TestProps().from(cli);

        assertEquals(10, (int) propsObject.intProp);
        assertEquals("foobar", propsObject.stringProp);
    }
    @Test
    public void from() throws Exception {
        Map map = new HashMap();
        map.put("int.prop", 789);
        map.put("string.prop", "foo");

        TestProps propsObject = new TestProps().from(map);

        assertEquals(789, (int)propsObject.intProp);
        assertEquals("foo", propsObject.stringProp);

        Properties properties = new Properties();
        properties.put("double.prop", 1.456);
        properties.put("string.prop", "bar");

        propsObject.from(properties);

        assertEquals("bar", propsObject.stringProp);
        assertEquals(1.456, propsObject.doubleProp, 0.001);

        assertNull(propsObject.nullProp);
    }

    @Test
    public void fromWithNamespace() throws Exception {
        Map map = new HashMap();
        map.put("foo.bar.int.prop", 789);
        map.put("foo.bar.string.prop", "foo");
        map.put("foo.barbar.double.prop", 3.678);
        map.put("foofoo.bar.bool.prop", true);

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