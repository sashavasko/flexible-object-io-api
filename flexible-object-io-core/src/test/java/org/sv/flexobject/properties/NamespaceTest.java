package org.sv.flexobject.properties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NamespaceTest {

    @Test
    public void getSettingName() {
        assertEquals("foo", Namespace.EMPTY.getSettingName("foo"));
        assertEquals("foo.bar", Namespace.EMPTY.getSettingName("fooBar"));
        assertEquals("db.foo", Namespace.DB.getSettingName("foo"));
        assertEquals("db.foo.bar", Namespace.DB.getSettingName("fooBar"));
        Namespace subSpace1 = new Namespace(new Namespace("top"), "mongo");
        assertEquals("top.mongo.foo", subSpace1.getSettingName("foo"));
        assertEquals("top.mongo.foo.bar", subSpace1.getSettingName("fooBar"));
        Namespace subSpace2 = new Namespace(new Namespace("toptop"), subSpace1);
        assertEquals("toptop.mongo.foo", subSpace2.getSettingName("foo"));
        assertEquals("toptop.mongo.foo.bar", subSpace2.getSettingName("fooBar"));
    }

    @Test
    public void getPathName() {
        assertEquals("foo", Namespace.EMPTY.getPathName("foo"));
        assertEquals("foo.bar", Namespace.EMPTY.getPathName("fooBar"));
        assertEquals("foo.bar", Namespace.EMPTY.getPathName("foo", "bar"));
        assertEquals("db.foo", Namespace.DB.getPathName("foo"));
        assertEquals("db.foo.bar", Namespace.DB.getPathName("fooBar"));
        assertEquals("db.foo.bar", Namespace.DB.getPathName("foo", "bar"));
    }
}