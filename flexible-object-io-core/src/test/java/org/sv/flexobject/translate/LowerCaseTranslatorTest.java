package org.sv.flexobject.translate;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LowerCaseTranslatorTest {

    @Test
    public void apply() {
        assertEquals("blah_i_think_in_lowercase", new LowerCaseTranslator().apply("BLAH_i_Think_iN_lOwErcase"));
    }
}