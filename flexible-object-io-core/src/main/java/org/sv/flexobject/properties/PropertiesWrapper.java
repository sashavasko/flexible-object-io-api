package org.sv.flexobject.properties;


import org.apache.commons.cli.CommandLine;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.adapter.MapInAdapter;
import org.sv.flexobject.adapter.MapOutAdapter;
import org.sv.flexobject.adapter.OptionsInAdapter;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.translate.NamespaceTranslator;
import org.sv.flexobject.translate.SeparatorTranslator;
import org.sv.flexobject.translate.Translator;

import java.util.*;
import java.util.function.Supplier;

public abstract class PropertiesWrapper<T extends PropertiesWrapper> extends StreamableWithSchema {

    public PropertiesWrapper(){
        setDefaults();
    }

    public Translator getTranslator() {
        return new SeparatorTranslator(".");
    }

    public String getSettingName(String fieldName) {
        return getTranslator().apply(fieldName);
    }

    public List<String> listSettings() {
        List<String> settings = new ArrayList<>();

        for (SchemaElement e : Schema.getRegisteredSchema(getClass()).getFields()) {
            settings.add(getSettingName(e.getDescriptor().getName()));
        }
        return settings;
    }

    public Properties getProps() throws Exception {
        return (Properties) MapOutAdapter.builder()
                .forClass(Properties.class)
                .translator(getTranslator())
                .build().produce(this::save);
    }

    public Map getMap() throws Exception {
        return getMap(HashMap.class);
    }

    public Map getMap(Class<? extends Map> mapClass) throws Exception {
        return MapOutAdapter.builder()
                .forClass(mapClass)
                .translator(getTranslator())
                .build().produce(this::save);
    }

    public Map getMap(Supplier<Map> mapFactory) throws Exception {
        return MapOutAdapter.builder()
                .withFactory(mapFactory)
                .translator(getTranslator())
                .build().produce(this::save);
    }

    public T from(CommandLine source) throws Exception {
        OptionsInAdapter.builder().from(source).build().consume(this::load);
        return (T) this;
    }

    public T from(Map source) throws Exception {
        MapInAdapter.builder().from(source).translator(getTranslator()).build().consume(this::load);
        return (T) this;
    }

    public T from(Map source, String namespace) throws Exception {
        Translator composition = getTranslator().andThen(new NamespaceTranslator(namespace));
        MapInAdapter.builder().from(source).translator(composition).build().consume(this::load);
        return (T) this;
    }

    public T from(Map source, Translator nameTranslator) throws Exception {
        MapInAdapter.builder().from(source).translator(nameTranslator).build().consume(this::load);
        return (T) this;
    }

    public T from(InAdapter source) throws Exception {
        source.consume(this::load);
        return (T) this;
    }

    public abstract T setDefaults();

    @Override
    public boolean load(InAdapter input) throws Exception {
//        clear(); DO NOT WANT TO ERASE DEFAULTS IF SOMETHING IS NOT SET IN INPUT
        return getSchema().load(this, input) != null;
    }
}
