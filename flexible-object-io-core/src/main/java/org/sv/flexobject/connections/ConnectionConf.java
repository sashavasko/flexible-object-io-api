package org.sv.flexobject.connections;


import org.sv.flexobject.properties.PropertiesWrapper;
import org.sv.flexobject.translate.Translator;

public abstract class ConnectionConf<SELF extends PropertiesWrapper> extends PropertiesWrapper<SELF> {

    @Override
    public Translator getTranslator() {
        return Translator.identity();
    }

}
