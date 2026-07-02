package org.sv.flexobject.translate;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UperCaseTranslatorTest {

    @Test
    public void apply() {
        assertEquals("BLAH_I_THINK_IN_UPPERCASE", new UpperCaseTranslator().apply("BLAH_i_Think_iN_upperCaSe"));

    }
}