package org.sv.flexobject.schema;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.StreamableWithSchema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AbstractSchema {
    protected String name;
    protected SchemaElement[] fields;
    protected Map<String, SchemaElement> fieldsByName = new HashMap<>();
    protected Map<String, Integer> paramNamesXref = new HashMap<>();

    public AbstractSchema(Class<?> dataClass) {
        this.name = dataClass.getName();
    }

    protected void addField(SchemaElement field, int order){
        fields[order] = field;
        fieldsByName.put(field.getDescriptor().getName(), field);
    }

    protected void setFields(List<SchemaElement> fieldList){
        this.fields = new SchemaElement[fieldList.size()];
        int i = 0;
        for (SchemaElement field : fieldList)
            addField(field, i++);
    }

    protected void initParamXref() {
        for (SchemaElement f  : fields){
            paramNamesXref.put(f.getDescriptor().getName(), f.getDescriptor().getOrder()+1);
        }
    }
    public String getName() {
        return name;
    }

    public String getSimpleName() {
        return name.substring(name.lastIndexOf('.')+1);
    }

    public String getNamespace() {
        return name.substring(0, name.lastIndexOf('.'));
    }

    public SchemaElement[] getFields() {
        return fields;
    }

    public AbstractFieldDescriptor getFieldDescriptor(Enum<?> e) {
        return fields[e.ordinal()].getDescriptor();
    }

    public AbstractFieldDescriptor getFieldDescriptor(int i) {
        return fields[i].getDescriptor();
    }

    public AbstractFieldDescriptor getFieldDescriptor(String fieldName) {
        return fieldsByName.get(fieldName).getDescriptor();
    }

    public Map<String, Integer> getParamNamesXref() {
        return paramNamesXref;
    }

    public void clear(Object datum) throws SchemaException {
        for (SchemaElement field : fields) {
            field.getDescriptor().clear(datum);
        }
    }

    public boolean loadFields(Object datum, InAdapter input) throws SchemaException {
        for (SchemaElement field : fields) {
            field.getDescriptor().load(datum, input);
        }
        return true;
    }

    public boolean saveFields(Object datum, OutAdapter output) throws Exception {
        for (SchemaElement field : fields){
            field.getDescriptor().save(datum, output);
        }
        return output.saveIfYouShould();
    }

    public boolean compareFields(Object o1, Object o2){
        try {
            for (SchemaElement field : fields) {
                AbstractFieldDescriptor descriptor = field.getDescriptor();
                Object value = descriptor.get(o1);
                Object otherValue = descriptor.get(o2);
                if (value != null) {
                    if (otherValue == null)
                        return false;

                    if (value.getClass().isArray() != otherValue.getClass().isArray())
                        return false;

                    if (value.getClass().isArray()) {
                        if (!Arrays.equals((Object[])value, (Object[])otherValue)) {
                            return false;
                        }
                    } else if (value instanceof Map) {
                        if (otherValue instanceof Map){
                            return ((Map)value).entrySet().equals(((Map)otherValue).entrySet());
                        } else
                            return false;
                    } else if (!value.equals(otherValue))
                        return false;
                } else if (otherValue != null)
                    return false;
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public boolean isEmpty(StreamableWithSchema o) throws SchemaException {
        for (SchemaElement field : fields) {
            AbstractFieldDescriptor descriptor = field.getDescriptor();
            if (!descriptor.isEmpty(o))
                return false;
        }
        return true;
    }


}
