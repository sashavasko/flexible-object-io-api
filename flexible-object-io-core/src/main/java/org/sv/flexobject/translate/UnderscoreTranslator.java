package org.sv.flexobject.translate;

public class UnderscoreTranslator extends SeparatorTranslator{

    public UnderscoreTranslator() {
        super("_");
    }

    public static String translate(String s){
        return translate(s, "_", new StringBuilder());
    }

}
