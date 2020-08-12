package org.sv.flexobject.translate;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;

public class FullCamelCaseTranslator implements Translator{

    public static final char[] DEFAULT_DELIMITERS = new char[]{'_', '.', '-', ' ', '\t', '\n'};
    char[] delimiters = DEFAULT_DELIMITERS;

    public FullCamelCaseTranslator(char[] delimiters) {
        this.delimiters = delimiters;
    }

    public FullCamelCaseTranslator() {
    }

    public static String translate(String s) {
        return translate(s, DEFAULT_DELIMITERS);
    }

    public static String translate(String s, char[] delimiters){
        String result = WordUtils.capitalize(s, delimiters);
        for (char c : delimiters) {
            String newResult;
            while ((newResult = StringUtils.remove(result, c)) != result) result = newResult;
        }
        return result;
    }

    @Override
    public String apply(String s) {
        return translate(s, delimiters);
    }
}
