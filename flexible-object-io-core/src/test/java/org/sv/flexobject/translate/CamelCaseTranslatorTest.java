package org.sv.flexobject.translate;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CamelCaseTranslatorTest {

    @Test
    public void apply() {
        assertEquals("thisIsCamelCaseForSureIThink", CamelCaseTranslator.translate("This is-camel.case_for\tsure\nI think"));
        assertEquals("thisIsCamelCaseForSureIThink", CamelCaseTranslator.translate("This  is--camel..case__for\t\tsure\n\nI think"));
    }
}