package org.sv.flexobject.translate;

public class LowerCaseTranslator implements Translator{
    @Override
    public String apply(String s) {
        return s.toLowerCase();
    }
}
