package org.sv.flexobject.translate;

public class SeparatorTranslator implements Translator{

    public static final String DEFAULT_SEPARATOR = ".";
    String separator = DEFAULT_SEPARATOR;

    StringBuilder sb = new StringBuilder();

    public SeparatorTranslator() {
    }

    public SeparatorTranslator(String separator) {
        this.separator = separator;
    }

    public static String translate(String s){
        return translate(s, DEFAULT_SEPARATOR, new StringBuilder());
    }

    public static String translate(String s, String separator){
        return translate(s, separator, new StringBuilder());
    }

    public static String translate(String s, String separator, StringBuilder sb){
        boolean insideNumber = false;
        for (int i = 0 ; i < s.length() ; ++i){
            char c = s.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append(separator).append(Character.toLowerCase(c));
                insideNumber = false;
            }else if (!insideNumber){
                if (Character.isDigit(c)) {
                    sb.append(separator);
                    insideNumber = true;
                }
                sb.append(c);
            } else {
                if (!Character.isDigit(c))
                    insideNumber = false;
                sb.append(c);
            }
        }
        return sb.toString();
    }

    @Override
    public String apply(String s) {
//        sb.setLength(0);
        return translate(s, separator, new StringBuilder());
    }
}
