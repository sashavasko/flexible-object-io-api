package org.sv.flexobject.hadoop.adapter;

import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.adapter.DynamicOutAdapter;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.schema.DataTypes;

public class ConfigurationOutAdapter extends GenericOutAdapter<Configuration> implements DynamicOutAdapter {

    @Override
    public Object put(String translatedFieldName, Object value) throws Exception {
        getCurrent().set(translatedFieldName, DataTypes.stringConverter(value));
        return value;
    }
}
