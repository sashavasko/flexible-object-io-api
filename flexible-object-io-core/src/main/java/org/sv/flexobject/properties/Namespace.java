package org.sv.flexobject.properties;

import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.translate.NamespaceTranslator;
import org.sv.flexobject.translate.SeparatorTranslator;
import org.sv.flexobject.translate.Translator;

import java.util.Collection;

public class Namespace {

    public static final Namespace EMPTY = new Namespace("");
    public static final Namespace DEFAULT = new Namespace("sv");
    public static final Namespace DB = new Namespace("db");
    public static final String DEFAULT_SEPARATOR = ".";

    private Namespace parent;
    private String name;
    private String separator = DEFAULT_SEPARATOR;

    public Namespace(Namespace parent, String namespace) {
        this.parent = parent;
        this.separator = parent.separator;
        this.name = namespace;
    }

    public Namespace(Namespace parent, Namespace subSpace) {
        this.parent = parent;
        this.separator = subSpace.separator;
        this.name = subSpace.name;
    }

    public Namespace(String namespace) {
        this.name = namespace;
    }

    public Namespace(String namespace, String separator) {
        this.name = namespace;
        this.separator = separator;
    }

    public static Namespace forPath(String separator, String pathStart, String ... path){
        Namespace ns = new Namespace(pathStart, separator);
        for (String pathElement : path){
            ns = new Namespace(ns, pathElement);
        }
        return ns;
    }

    public static Namespace forPath(Collection<String> path){
        return forPath(DEFAULT_SEPARATOR, path);
    }

    public static Namespace forPath(Collection<String> path, String subSpace){
        return path == null || path.isEmpty()?
                new Namespace(subSpace)
                : new Namespace(forPath(DEFAULT_SEPARATOR, path), subSpace);
    }

    public static Namespace forPath(String separator, Collection<String> path){
        Namespace ns = null;
        for (String pathElement : path){
            if (ns == null)
                ns = new Namespace(pathElement, separator);
            else
                ns = new Namespace(ns, pathElement);
        }
        return ns;
    }

    public String getName(){
        return name;
    }

    public Namespace getParent() {
        return parent;
    }

    public String getSeparator() {
        return separator;
    }

    public String getNamespace() {
        return parent == null ? name : parent.getNamespace() + separator + name;
    }

    public static Translator getTranslator(String namespace, String separator) {
        return new SeparatorTranslator(separator).andThen(new NamespaceTranslator(namespace));
    }

    public String getSettingName(String fieldName){
        return getTranslator(getNamespace(), separator).apply(fieldName);
    }

    public String getPathName(String ... pathElements){
        StringBuilder path = new StringBuilder();
        for (String pathElement : pathElements) {
            path.append(separator);
            SeparatorTranslator.translate(pathElement, separator, path);
        }

        return StringUtils.isNotBlank(getNamespace()) ? getNamespace() + path.toString() : path.deleteCharAt(0).toString();
    }

}
