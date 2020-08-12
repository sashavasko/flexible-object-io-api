package org.sv.flexobject.translate;

public class UpperCaseTranslator implements Translator{
    @Override
    public String apply(String s) {
        return s.toUpperCase();
    }
}
