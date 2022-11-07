package org.sv.flexobject.properties;


import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.adapter.MapInAdapter;
import org.sv.flexobject.adapter.MapOutAdapter;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.annotations.NonStreamableField;
import org.sv.flexobject.translate.Translator;
import org.sv.flexobject.util.InstanceFactory;

import java.util.*;
import java.util.function.Supplier;

public abstract class NamespacePropertiesWrapper<T extends NamespacePropertiesWrapper> extends PropertiesWrapper<T> {

    @NonStreamableField
    private Namespace namespace;

    protected String getSubNamespace(){
        throw new RuntimeException("Attempt to instantiate Configuration object of class: " + getClass().getName() + " without specifying sub-namespace. Please override getSubnamespace()");
    };

    protected static Namespace getParentNamespace(Class<? extends NamespacePropertiesWrapper> clazz){
        Class superclass = clazz.getSuperclass();
        if (superclass == NamespacePropertiesWrapper.class)
            return Namespace.getDefaultNamespace();
        else {
            NamespacePropertiesWrapper parent = (NamespacePropertiesWrapper) InstanceFactory.get(superclass);
            return parent.getNamespace();
        }
    }

    protected static Namespace makeMyNamespace(Namespace parent, String subNamespace){
        return new Namespace(parent, subNamespace);
    }

    public NamespacePropertiesWrapper() {
        namespace = Namespace.getDefaultNamespace();
    }

    public NamespacePropertiesWrapper(String subNamespace) {
        namespace = new Namespace(Namespace.getDefaultNamespace(), subNamespace);
    }

    public NamespacePropertiesWrapper(Namespace parent, String subNamespace) {
        this.namespace = makeMyNamespace(parent, subNamespace);
    }

    public NamespacePropertiesWrapper(Namespace parent) {
        super();
        this.namespace = makeMyNamespace(parent, getSubNamespace());
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public void setNamespace(Namespace parent, String subNamespace){
        this.namespace = makeMyNamespace(parent == null ? Namespace.getDefaultNamespace() : parent, subNamespace);
    }

    public void setNamespace(Namespace parent){
        this.namespace = makeMyNamespace(parent == null ? Namespace.getDefaultNamespace() : parent, getSubNamespace());
    }

    @Override
    public Translator getTranslator() {
        return namespace.getTranslator();
    }

    @Override
    public Map toHashMap() throws Exception {
        return getMap(HashMap.class);
    }

    @Override
    public Map toMap(Supplier mapFactory) throws Exception {
        return getMap(mapFactory);
    }

    @Override
    public Map toMap(Class outputMapClass) throws Exception {
        return getMap(outputMapClass);
    }

    @Override
    public StreamableWithSchema fromMap(Map map) throws Exception {
        return from(map);
    }
}
