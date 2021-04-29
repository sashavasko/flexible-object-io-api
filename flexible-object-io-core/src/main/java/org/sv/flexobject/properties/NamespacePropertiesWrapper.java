package org.sv.flexobject.properties;


import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.adapter.MapInAdapter;
import org.sv.flexobject.adapter.MapOutAdapter;
import org.sv.flexobject.schema.annotations.NonStreamableField;
import org.sv.flexobject.translate.Translator;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public abstract class NamespacePropertiesWrapper<T extends NamespacePropertiesWrapper> extends PropertiesWrapper<T> {

    @NonStreamableField
    private Namespace namespace;

    public NamespacePropertiesWrapper(String namespace) {
        this.namespace = new Namespace(Namespace.getDefaultNamespace(), namespace);
    }

    public NamespacePropertiesWrapper(Namespace parent, String namespace) {
        this.namespace = new Namespace(parent, namespace);
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public Translator getTranslator() {
        return namespace.getTranslator();
    }

    public String getSettingName(String fieldName){
        return getTranslator().apply(fieldName);
    }

    @Override
    public Properties getProps() throws Exception {
        return (Properties) MapOutAdapter.builder()
                .forClass(Properties.class)
                .translator(getTranslator())
                .build().produce(this::save);
    }

    @Override
    public Map getMap() throws Exception {
        return MapOutAdapter.builder()
                .forClass(HashMap.class)
                .translator(getTranslator())
                .build().produce(this::save);
    }

    @Override
    public Map getMap(Class<? extends Map> mapClass) throws Exception {
        return MapOutAdapter.builder()
                .forClass(mapClass)
                .translator(getTranslator())
                .build().produce(this::save);
    }

    @Override
    public Map getMap(Supplier<Map> mapFactory) throws Exception {
        return MapOutAdapter.builder()
                .withFactory(mapFactory)
                .translator(getTranslator())
                .build().produce(this::save);
    }

    @Override
    public T from(Map source) throws Exception {
        MapInAdapter.builder().from(source).translator(getTranslator()).build().consume(this::load);
        return (T) this;
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
