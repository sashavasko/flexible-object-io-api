package org.sv.flexobject.translate;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LowerCaseTranslatorTest {

    @Test
    public void apply() {
        assertEquals("blah_i_think_in_lowercase", new LowerCaseTranslator().apply("BLAH_i_Think_iN_lOwErcase"));
    }
}