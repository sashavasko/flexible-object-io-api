package org.sv.flexobject.translate;

public class NamespaceTranslator implements Translator {

    String namespace;
    String separator = ".";

    public NamespaceTranslator(String namespace, String separator) {
        this.namespace = namespace;
        this.separator = separator;
    }

    public NamespaceTranslator(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public String apply(String s) {
        return namespace + separator + s;
    }
}
