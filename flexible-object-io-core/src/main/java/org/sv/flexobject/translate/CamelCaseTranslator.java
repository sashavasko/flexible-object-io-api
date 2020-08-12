package org.sv.flexobject.translate;

public class CamelCaseTranslator extends FullCamelCaseTranslator{

    public CamelCaseTranslator(char[] delimiters) {
        super(delimiters);
    }

    public CamelCaseTranslator() {
    }

    public static String lowercaseFirst(String s){
        return Character.toLowerCase(s.charAt(0)) + s.substring(1);
    }

    public static String translate (String s){
        return lowercaseFirst(FullCamelCaseTranslator.translate(s));
    }

    public static String translate (String s, char[] delimiters){
        return lowercaseFirst(FullCamelCaseTranslator.translate(s, delimiters));
    }

    @Override
    public String apply(String s) {
        return translate(s, delimiters);
    }
}
