package org.sv.flexobject.hadoop.mapreduce.input;


import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.util.InstanceFactory;

public class SourceConf<SELF extends HadoopPropertiesWrapper> extends InputConf<SELF> {
    public static final String SUBNAMESPACE = "input.source";

    protected Class<? extends SourceBuilder> sourceBuilderClass;

    public SourceConf() {
        super();
    }

    public SourceConf(String namespace) {
        super(namespace);
    }

    @Override
    public String getSubNamespace() {
        return SUBNAMESPACE;
    }

    @Override
    public SELF setDefaults() {
        return (SELF)this;
    }

    public SourceBuilder getSourceBuilder() throws IllegalAccessException, InstantiationException, IllegalArgumentException {
        if (sourceBuilderClass == null)
            throw new IllegalArgumentException("Missing Source Builder class");
        return InstanceFactory.get(sourceBuilderClass);
    }
}
