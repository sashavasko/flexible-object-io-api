package org.sv.flexobject.properties;


import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.adapter.MapInAdapter;
import org.sv.flexobject.adapter.MapOutAdapter;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.translate.NamespaceTranslator;
import org.sv.flexobject.translate.Translator;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class PropertiesWrapper<T extends PropertiesWrapper> extends StreamableWithSchema {

    public PropertiesWrapper(){}

    public Properties getProps() throws Exception {
        return (Properties) MapOutAdapter.produce(Properties.class, this::save);
    }

    public Map getMap() throws Exception {
        return MapOutAdapter.produce(HashMap.class, this::save);
    }

    public Map getMap(Class<? extends Map> mapClass) throws Exception {
        return MapOutAdapter.produce(mapClass, this::save);
    }

    public Map getMap(Supplier<Map> mapFactory) throws Exception {
        return MapOutAdapter.produce(mapFactory, this::save);
    }

    public T from(Map source) throws Exception {
        new MapInAdapter(new SingleValueSource<>(source)).consume(this::load);
        return (T) this;
    }

    public T from(Map source, String namespace) throws Exception {
        GenericInAdapter adapter = new MapInAdapter(new SingleValueSource<>(source));
        adapter.setParam(GenericInAdapter.PARAMS.fieldNameTranslator, new NamespaceTranslator(namespace));
        adapter.consume(this::load);
        return (T) this;
    }

    public T from(Map source, Translator nameTranslator) throws Exception {
        GenericInAdapter adapter = new MapInAdapter(new SingleValueSource<>(source));
        adapter.setParam(GenericInAdapter.PARAMS.fieldNameTranslator, nameTranslator);
        adapter.consume(this::load);
        return (T) this;
    }

    public T from(InAdapter source) throws Exception {
        source.consume(this::load);
        return (T) this;
    }
}
