package org.sv.flexobject.schema;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.Loadable;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.Savable;

import java.util.HashMap;
import java.util.Map;

public class Schema {

    protected String name;
    protected SchemaElement[] fields;
    protected Map<String, Integer> paramNamesXref = new HashMap<>();

    public Schema(String name, SchemaElement[] fields) {
        this.name = name;
        this.fields = fields;
        for (SchemaElement f  : fields){
            paramNamesXref.put(f.getDescriptor().getName(), f.getDescriptor().getOrder()+1);
        }
    }

    public String getName() {
        return name;
    }

    public SchemaElement[] getFields() {
        return fields;
    }

    public Map<String, Integer> getParamNamesXref() {
        return paramNamesXref;
    }

    public boolean load(Loadable datum, InAdapter input) throws Exception {
        for (SchemaElement field : fields){
            field.getDescriptor().load(datum, input);
        }
        return true;
    }

    public boolean save(Savable datum, OutAdapter output) throws Exception {
        for (SchemaElement field : fields){
            field.getDescriptor().save(datum, output);
        }
        return true;
    }

    public boolean compareFields(Object o1, Object o2){
        try {
            for (SchemaElement field : fields) {
                FieldDescriptor descriptor = field.getDescriptor();
                Object value = descriptor.get(o1);
                if (value != null) {
                    if (!value.equals(descriptor.get(o2)))
                        return false;
                }else if (descriptor.get(o2) != null)
                    return false;
            }
        }catch (Exception e) {
            return false;
        }

        return true;
    }
}
