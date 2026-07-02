package org.sv.flexobject.translate;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NamespaceTranslatorTest {

    @Test
    public void apply() {
        assertEquals("name.space.property", new NamespaceTranslator("name.space", ".").apply("property"));
        assertEquals("name_space_property", new NamespaceTranslator("name_space", "_").apply("property"));
        assertEquals("namespace::property", new NamespaceTranslator("namespace", "::").apply("property"));
    }
}