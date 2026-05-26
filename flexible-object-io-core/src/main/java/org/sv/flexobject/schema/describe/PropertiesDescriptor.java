package org.sv.flexobject.schema.describe;


import org.sv.flexobject.properties.PropertiesWrapper;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.annotations.KeyType;
import org.sv.flexobject.schema.annotations.ValueClass;
import org.sv.flexobject.translate.SeparatorTranslator;

import java.util.HashMap;
import java.util.Map;

public class PropertiesDescriptor extends BasicDescriptor<PropertiesDescriptor> {
    Class<?> clazz;

    @KeyType(type = DataTypes.string)
    @ValueClass(valueClass = PropertiesWrapper.class)
    Map<String, PropertyDescriptor> properties = new HashMap<>();

    public PropertiesDescriptor() {
    }

    public PropertiesDescriptor(Class<?> instanceClass) {
        clazz = instanceClass;
        displayName = SeparatorTranslator.translate(instanceClass.getSimpleName(), " ").trim();

        from(instanceClass.getAnnotation(PropertyDescription.class));

        Schema schema = Schema.getRegisteredSchema(instanceClass);
        for (SchemaElement field : schema.getFields()) {
            addField(new PropertyDescriptor(instanceClass, field));
        }
    }


    public PropertiesDescriptor addField(PropertyDescriptor desc) {
        properties.put(desc.getPropertyName(), desc);
        return this;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public Map<String, PropertyDescriptor> getProperties() {
        return properties;
    }
}
