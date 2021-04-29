package org.sv.flexobject.adapter;

import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.translate.Translator;

import java.util.Map;

public class MapInAdapter extends GenericInAdapter<Map> implements DynamicInAdapter{

    public MapInAdapter() {
    }

    public MapInAdapter(Source<Map> source) {
        super(source);
    }

    @Override
    public Object get(Object translatedFieldName) {
        return getCurrent().get(translatedFieldName);
    }

    public static class Builder {
        Map map;
        Source<Map> source;
        Translator fieldNameTranslator;

        public Builder from(Map map){
            this.map = map;
            return this;
        }

        public Builder fromSource(Source<Map> source){
            this.source = source;
            return this;
        }

        public Builder translator(Translator fieldNameTranslator){
            this.fieldNameTranslator = fieldNameTranslator;
            return this;
        }

        public MapInAdapter build(){
            MapInAdapter adapter = new MapInAdapter();
            if (fieldNameTranslator != null)
                adapter.setParam(GenericInAdapter.PARAMS.fieldNameTranslator, fieldNameTranslator);
            if (map != null)
                adapter.setParam(PARAMS.source, new SingleValueSource<>(map));
            else if (source != null)
                adapter.setParam(PARAMS.source, source);
            return adapter;
        }
    }

    public static Builder builder(){
        return new Builder();
    }
}
