package org.sv.flexobject.translate;

public class UnderscoreTranslator implements Translator{
    StringBuilder sb = new StringBuilder();

    public static String translate(String s){
        return translate(s, new StringBuilder());
    }

    public static String translate(String s, StringBuilder sb){
        sb.setLength(0);
        boolean insideNumber = false;
        for (int i = 0 ; i < s.length() ; ++i){
            char c = s.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append('_').append(Character.toLowerCase(c));
                insideNumber = false;
            }else if (!insideNumber){
                if (Character.isDigit(c)) {
                    sb.append('_');
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
        return translate(s, sb);
    }
}
