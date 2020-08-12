package org.sv.flexobject.translate;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UnderscoreTranslatorTest {

    @Test
    public void apply() {
        assertEquals("blah_foo_bar", UnderscoreTranslator.translate("blahFooBar"));
        assertEquals("blah_foo_bar_123", UnderscoreTranslator.translate("blahFooBar123"));
        assertEquals("blah_foo_bar_123_again_456", UnderscoreTranslator.translate("blahFooBar123Again456"));
    }
}