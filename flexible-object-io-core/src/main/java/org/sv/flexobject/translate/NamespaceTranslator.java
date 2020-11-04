package org.sv.flexobject.translate;

import org.apache.commons.lang3.StringUtils;

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
        return StringUtils.isNotBlank(namespace) ? namespace + separator + s : s;
    }
}
