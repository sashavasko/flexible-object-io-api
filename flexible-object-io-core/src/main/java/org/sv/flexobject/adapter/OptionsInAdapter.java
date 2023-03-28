package org.sv.flexobject.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.translate.Translator;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Map;
import java.util.Properties;

public class OptionsInAdapter extends GenericInAdapter<CommandLine> implements DynamicInAdapter{

    public OptionsInAdapter() {
    }

    public OptionsInAdapter(CommandLine cli) {
        super(new SingleValueSource<>(cli));
    }

    @Override
    public Object get(Object translatedFieldName) {
        return getCurrent().getOptionValue((String) translatedFieldName);
    }

    @Override
    public JsonNode getJson(String fieldName) throws Exception {
        boolean isMap = false;
        for (Option option : getCurrent().getOptions()){
            if (fieldName.equals(option.getOpt()) || fieldName.equals(option.getLongOpt())) {
                if (option.hasArgName() && option.getArgName().contains("=")) {
                    isMap = true;
                    break;
                }
            }
        }
        if (!isMap) {
            String[] values = getCurrent().getOptionValues(fieldName);
            if (values != null) {
                ArrayNode jsonArray = JsonNodeFactory.instance.arrayNode(values.length);
                for (String value : values)
                    jsonArray.add(value);
                return jsonArray;
            }
        }
        Properties properties = getCurrent().getOptionProperties(fieldName);
        if (properties != null){
            ObjectNode json = JsonNodeFactory.instance.objectNode();
            for (Map.Entry<Object, Object> e : properties.entrySet()){
                if (e.getValue() instanceof String){
                    String s = ((String) e.getValue()).trim();
                    if (!s.startsWith("{") && !s.startsWith("[") && !s.contains(",") ) {
                        json.put((String) e.getKey(), s);
                        continue;
                    }
                }
                json.set((String) e.getKey(), DataTypes.jsonConverter(e.getValue()));
            }
            return json;
        }
        return null;
    }

    public static class Builder {
        CommandLine map;
        Source<CommandLine> source;
        Translator fieldNameTranslator;

        public Builder from(CommandLine map){
            this.map = map;
            return this;
        }

        public Builder fromSource(Source<CommandLine> source){
            this.source = source;
            return this;
        }

        public Builder translator(Translator fieldNameTranslator){
            this.fieldNameTranslator = fieldNameTranslator;
            return this;
        }

        public OptionsInAdapter build(){
            OptionsInAdapter adapter = InstanceFactory.get(OptionsInAdapter.class);
            if (fieldNameTranslator != null)
                adapter.setParam(PARAMS.fieldNameTranslator, fieldNameTranslator);
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
